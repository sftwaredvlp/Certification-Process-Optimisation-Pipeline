"""Derive pre-audit checklist from historical failure patterns."""

import sqlite3
import pandas as pd
import os


def get_db_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(script_dir), 'data', 'certification.db')


def analyze_failures(conn):
    print("--- Failure Analysis ---\n")
    
    stats = pd.read_sql_query("""
        SELECT 
            COUNT(*) AS total_failures,
            COUNT(DISTINCT application_id) AS affected_apps
        FROM v_failures
    """, conn).iloc[0]
    
    print(f"Total audit failures: {stats['total_failures']}")
    print(f"Applications affected: {stats['affected_apps']}")
    
    by_module = pd.read_sql_query("""
        SELECT mid_module, COUNT(*) AS failures
        FROM v_failures GROUP BY mid_module
    """, conn)
    print(f"\nBy module:")
    for _, r in by_module.iterrows():
        print(f"  Module {r['mid_module']}: {r['failures']} failures")
    
    return stats


def calculate_impact(conn):
    print("\n--- Impact Analysis ---\n")
    
    impact = pd.read_sql_query("""
        SELECT 
            ROUND(AVG(CASE WHEN passed_first_time = 0 THEN turnaround_days END), 1) AS failed_avg,
            ROUND(AVG(CASE WHEN passed_first_time = 1 THEN turnaround_days END), 1) AS passed_avg
        FROM v_application_details
        WHERE certification_date IS NOT NULL
    """, conn).iloc[0]
    
    delay_per_failure = impact['failed_avg'] - impact['passed_avg']
    print(f"Avg turnaround (first-time pass): {impact['passed_avg']} days")
    print(f"Avg turnaround (with failures):   {impact['failed_avg']} days")
    print(f"Delay caused by failures:         {delay_per_failure:.1f} days")
    
    revisions = pd.read_sql_query("""
        SELECT 
            total_revisions,
            ROUND(AVG(turnaround_days), 1) AS avg_days,
            COUNT(*) AS apps
        FROM v_application_details
        WHERE certification_date IS NOT NULL AND total_revisions > 0
        GROUP BY total_revisions
    """, conn)
    
    print(f"\nDays added per revision:")
    baseline = impact['passed_avg']
    for _, r in revisions.iterrows():
        delta = r['avg_days'] - baseline
        per_rev = delta / r['total_revisions']
        print(f"  {r['total_revisions']} revisions: +{delta:.0f} days (~{per_rev:.0f} days/revision)")
    
    return delay_per_failure


def build_checklist(conn, module=None):
    where = f"WHERE mid_module = '{module}'" if module else ""
    title = f"Module {module}" if module else "All Modules"
    
    df = pd.read_sql_query(f"""
        SELECT 
            failure_reason,
            COUNT(*) AS occurrences,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM v_failures {where}), 1) AS pct
        FROM v_failures
        {where}
        GROUP BY failure_reason
        ORDER BY occurrences DESC
    """, conn)
    
    df['cumulative_pct'] = df['pct'].cumsum()
    
    def assign_priority(row):
        if row['cumulative_pct'] <= 50:
            return 'HIGH'
        elif row['cumulative_pct'] <= 80:
            return 'MEDIUM'
        return 'LOW'
    
    df['priority'] = df.apply(assign_priority, axis=1)
    
    return df, title


def failure_to_action(reason):
    actions = {
        'Technical file incomplete': 'Verify technical file contains all sections per MID Annex requirements',
        'Documentation inconsistencies': 'Cross-check all document references and version numbers',
        'Test report gaps': 'Confirm test reports cover all applicable MID essential requirements',
        'Metrological requirements unclear': 'Review metrological characteristics against MID Annex MI-001 to MI-010',
        'Software documentation missing': 'Include software architecture, version control, and validation records',
        'Durability evidence insufficient': 'Provide durability test results or field performance data',
        'Marking/labelling non-compliant': 'Check CE marking, NB number, and instrument labelling requirements',
        'EMC test results missing': 'Include EMC test reports per EN 61326 or equivalent',
        'Training records missing': 'Verify training records for all personnel in scope',
        'Internal audit gaps': 'Review internal audit schedule and findings closure',
        'Calibration records outdated': 'Check calibration status of all measurement equipment',
        'Non-conformance handling unclear': 'Document NCR process with examples of recent closures',
        'Production process not documented': 'Map production process with quality control points',
        'Supplier control insufficient': 'Include approved supplier list and evaluation records',
        'Management review incomplete': 'Provide recent management review minutes with actions',
        'Corrective action records missing': 'Document CAPA process with closure evidence'
    }
    return actions.get(reason, f'Review: {reason}')


def print_checklist(df, title):
    print(f"\n{'='*70}")
    print(f"PRE-AUDIT CHECKLIST: {title}")
    print(f"{'='*70}")
    
    for priority in ['HIGH', 'MEDIUM', 'LOW']:
        items = df[df['priority'] == priority]
        if items.empty:
            continue
        print(f"\n[{priority} PRIORITY]")
        for _, row in items.iterrows():
            print(f"\n  □ {failure_to_action(row['failure_reason'])}")
            print(f"    Issue: {row['failure_reason']}")
            print(f"    Frequency: {row['occurrences']} ({row['pct']}%)")


def estimate_savings(conn):
    print(f"\n{'='*70}")
    print("POTENTIAL IMPACT")
    print(f"{'='*70}")
    
    stats = pd.read_sql_query("""
        SELECT 
            COUNT(*) AS total_apps,
            SUM(CASE WHEN passed_first_time = 0 THEN 1 ELSE 0 END) AS failed_apps,
            SUM(total_revisions) AS total_revisions
        FROM certification_results
        WHERE certification_date IS NOT NULL
    """, conn).iloc[0]
    
    failed_apps = stats['failed_apps']
    
    print(f"\nCurrent state:")
    print(f"  Applications with failures: {failed_apps}")
    print(f"  Total revisions: {stats['total_revisions']}")
    
    top_failures = pd.read_sql_query("""
        SELECT SUM(n) as top3_count FROM (
            SELECT COUNT(*) as n FROM v_failures 
            GROUP BY failure_reason ORDER BY n DESC LIMIT 3
        )
    """, conn).iloc[0]['top3_count']
    
    total_failures = pd.read_sql_query("SELECT COUNT(*) as n FROM v_failures", conn).iloc[0]['n']
    top3_pct = top_failures / total_failures
    
    prevented = int(total_failures * top3_pct * 0.5)  # assume 50% prevention
    days_saved = prevented * 12
    
    print(f"\nIf checklist prevents 50% of top 3 issues:")
    print(f"  Failures prevented: ~{prevented}")
    print(f"  Days saved: ~{days_saved}")
    
    current_rate = (stats['total_apps'] - failed_apps) / stats['total_apps'] * 100
    projected_rate = (stats['total_apps'] - failed_apps + prevented * 0.8) / stats['total_apps'] * 100
    print(f"\n  Current success rate:   {current_rate:.1f}%")
    print(f"  Projected success rate: {projected_rate:.1f}%")


def export_csv(conn):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(os.path.dirname(script_dir), 'data')
    
    df = pd.read_sql_query("""
        SELECT 
            failure_reason,
            mid_module,
            COUNT(*) AS occurrences,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM v_failures), 1) AS pct_total
        FROM v_failures
        GROUP BY failure_reason, mid_module
        ORDER BY occurrences DESC
    """, conn)
    
    df['checklist_item'] = df['failure_reason'].apply(failure_to_action)
    output_path = os.path.join(output_dir, 'checklist_items.csv')
    df.to_csv(output_path, index=False)
    print(f"\nExported: {output_path}")
    
    return output_path


def main():
    conn = sqlite3.connect(get_db_path())
    
    try:
        analyze_failures(conn)
        calculate_impact(conn)
        
        for module in [None, 'B', 'D']:
            df, title = build_checklist(conn, module)
            print_checklist(df, title)
        
        estimate_savings(conn)
        export_csv(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
