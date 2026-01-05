"""
KPI calculations for MID certification analytics.

Metrics: success rate, turnaround time, revisions, failure rate, throughput.
"""

import sqlite3
import pandas as pd
import os


def get_db_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(script_dir), 'data', 'certification.db')


def print_section(title):
    print(f"\n--- {title} ---")


def query(conn, sql):
    """Run query and return dataframe."""
    return pd.read_sql_query(sql, conn)


def show(conn, title, sql):
    """Run query and print results."""
    print_section(title)
    df = query(conn, sql)
    print(df.to_string(index=False))
    return df


def kpi_1_success_rate(conn):
    """First-time certification success rate."""
    
    show(conn, "1a. Overall Success Rate", """
        SELECT 
            COUNT(*) AS total,
            SUM(passed_first_time) AS first_time_passes,
            ROUND(AVG(passed_first_time) * 100, 1) AS success_pct
        FROM certification_results
        WHERE certification_date IS NOT NULL
    """)
    
    show(conn, "1b. Success by Module", """
        SELECT 
            a.mid_module,
            COUNT(*) AS apps,
            ROUND(AVG(cr.passed_first_time) * 100, 1) AS success_pct
        FROM applications a
        JOIN certification_results cr ON a.application_id = cr.application_id
        WHERE cr.certification_date IS NOT NULL
        GROUP BY a.mid_module
        ORDER BY success_pct DESC
    """)
    
    show(conn, "1c. Success by Manufacturer Size", """
        SELECT 
            c.manufacturer_size,
            COUNT(*) AS apps,
            ROUND(AVG(cr.passed_first_time) * 100, 1) AS success_pct
        FROM clients c
        JOIN applications a ON c.client_id = a.client_id
        JOIN certification_results cr ON a.application_id = cr.application_id
        WHERE cr.certification_date IS NOT NULL
        GROUP BY c.manufacturer_size
        ORDER BY success_pct DESC
    """)
    
    show(conn, "1d. Success by Instrument", """
        SELECT 
            a.instrument_type,
            COUNT(*) AS apps,
            ROUND(AVG(cr.passed_first_time) * 100, 1) AS success_pct
        FROM applications a
        JOIN certification_results cr ON a.application_id = cr.application_id
        WHERE cr.certification_date IS NOT NULL
        GROUP BY a.instrument_type
        ORDER BY success_pct DESC
    """)


def kpi_2_turnaround(conn):
    """Certification turnaround time in days."""
    
    show(conn, "2a. Overall Turnaround", """
        SELECT 
            COUNT(*) AS completed,
            ROUND(AVG(turnaround_days), 1) AS avg_days,
            MIN(turnaround_days) AS min_days,
            MAX(turnaround_days) AS max_days
        FROM v_application_details
        WHERE certification_date IS NOT NULL
    """)
    
    show(conn, "2b. Turnaround by Module", """
        SELECT 
            mid_module,
            COUNT(*) AS completed,
            ROUND(AVG(turnaround_days), 1) AS avg_days
        FROM v_application_details
        WHERE certification_date IS NOT NULL
        GROUP BY mid_module
    """)
    
    show(conn, "2c. First-Time Pass vs Revisions", """
        SELECT 
            CASE WHEN passed_first_time = 1 THEN 'First-time' ELSE 'Revisions' END AS outcome,
            COUNT(*) AS apps,
            ROUND(AVG(turnaround_days), 1) AS avg_days
        FROM v_application_details
        WHERE certification_date IS NOT NULL
        GROUP BY passed_first_time
    """)
    
    show(conn, "2d. Turnaround by Revision Count", """
        SELECT total_revisions, COUNT(*) AS apps, ROUND(AVG(turnaround_days), 1) AS avg_days
        FROM v_application_details
        WHERE certification_date IS NOT NULL
        GROUP BY total_revisions
        ORDER BY total_revisions
    """)


def kpi_3_revisions(conn):
    """Revisions per client analysis."""
    
    show(conn, "3a. Revisions by Manufacturer Size", """
        SELECT 
            c.manufacturer_size,
            COUNT(DISTINCT c.client_id) AS clients,
            SUM(cr.total_revisions) AS total_revs,
            ROUND(AVG(cr.total_revisions), 2) AS avg_per_app
        FROM clients c
        JOIN applications a ON c.client_id = a.client_id
        JOIN certification_results cr ON a.application_id = cr.application_id
        GROUP BY c.manufacturer_size
    """)
    
    show(conn, "3b. Top 10 Clients by Revisions", """
        SELECT 
            c.company_name,
            c.manufacturer_size AS size,
            COUNT(*) AS apps,
            SUM(cr.total_revisions) AS revisions
        FROM clients c
        JOIN applications a ON c.client_id = a.client_id
        JOIN certification_results cr ON a.application_id = cr.application_id
        GROUP BY c.client_id
        ORDER BY revisions DESC
        LIMIT 10
    """)
    
    show(conn, "3c. Revision Distribution", """
        SELECT 
            total_revisions AS revs,
            COUNT(*) AS apps,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM certification_results), 1) AS pct
        FROM certification_results
        GROUP BY total_revisions
        ORDER BY total_revisions
    """)


def kpi_4_failure_rate(conn):
    """Audit failure rate and reasons."""
    
    show(conn, "4a. Overall Failure Rate", """
        SELECT 
            COUNT(*) AS audits,
            SUM(CASE WHEN audit_status = 'FAIL' THEN 1 ELSE 0 END) AS failures,
            ROUND(SUM(CASE WHEN audit_status = 'FAIL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS fail_pct
        FROM audit_results
        WHERE audit_status != 'PENDING'
    """)
    
    show(conn, "4b. Failure Rate by Module", """
        SELECT 
            a.mid_module,
            COUNT(*) AS audits,
            ROUND(SUM(CASE WHEN ar.audit_status = 'FAIL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS fail_pct
        FROM audit_results ar
        JOIN applications a ON ar.application_id = a.application_id
        WHERE ar.audit_status != 'PENDING'
        GROUP BY a.mid_module
    """)
    
    show(conn, "4c. Top Failure Reasons", """
        SELECT failure_reason, COUNT(*) AS n
        FROM v_failures
        GROUP BY failure_reason
        ORDER BY n DESC
        LIMIT 10
    """)
    
    show(conn, "4d. Module B Failures", """
        SELECT failure_reason, COUNT(*) AS n
        FROM v_failures WHERE mid_module = 'B'
        GROUP BY failure_reason ORDER BY n DESC LIMIT 5
    """)
    
    show(conn, "4e. Module D Failures", """
        SELECT failure_reason, COUNT(*) AS n
        FROM v_failures WHERE mid_module = 'D'
        GROUP BY failure_reason ORDER BY n DESC LIMIT 5
    """)


def kpi_5_throughput(conn):
    """Monthly certification throughput."""
    
    show(conn, "5a. Monthly Trend", """
        SELECT month, certifications, first_time_passes, success_rate
        FROM v_monthly_throughput
        ORDER BY month
    """)
    
    show(conn, "5b. Quarterly Summary", """
        SELECT 
            substr(month, 1, 4) || '-Q' || 
            CASE 
                WHEN substr(month, 6, 2) IN ('01','02','03') THEN '1'
                WHEN substr(month, 6, 2) IN ('04','05','06') THEN '2'
                WHEN substr(month, 6, 2) IN ('07','08','09') THEN '3'
                ELSE '4'
            END AS quarter,
            SUM(certifications) AS certs,
            ROUND(AVG(success_rate), 1) AS avg_success
        FROM v_monthly_throughput
        GROUP BY quarter
    """)
    
    show(conn, "5c. By Instrument Type", """
        SELECT instrument_type, COUNT(*) AS certs, ROUND(AVG(passed_first_time) * 100, 1) AS success_pct
        FROM v_application_details
        WHERE certification_date IS NOT NULL
        GROUP BY instrument_type
        ORDER BY certs DESC
    """)


def summary(conn):
    """Executive summary."""
    print_section("EXECUTIVE SUMMARY")
    
    df = query(conn, """
        SELECT 
            COUNT(*) AS apps,
            SUM(passed_first_time) AS passes,
            ROUND(AVG(passed_first_time) * 100, 1) AS success_pct,
            ROUND(AVG(total_revisions), 2) AS avg_revs,
            ROUND(AVG(turnaround_days), 1) AS avg_days
        FROM v_application_details
        WHERE certification_date IS NOT NULL
    """)
    
    r = df.iloc[0]
    print(f"Completed: {r['apps']} applications")
    print(f"Success rate: {r['success_pct']}%")
    print(f"Avg revisions: {r['avg_revs']}")
    print(f"Avg turnaround: {r['avg_days']} days")
    
    print("\nModule comparison:")
    df_mod = query(conn, "SELECT * FROM v_module_comparison")
    for _, row in df_mod.iterrows():
        print(f"  {row['mid_module']}: {row['success_rate']}% success, {row['avg_days']} days")
    
    print("\nTop failures:")
    df_fail = query(conn, """
        SELECT failure_reason, COUNT(*) n FROM v_failures
        GROUP BY failure_reason ORDER BY n DESC LIMIT 3
    """)
    for _, row in df_fail.iterrows():
        print(f"  - {row['failure_reason']} ({row['n']})")


def main():
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    
    try:
        print(f"Database: {db_path}")
        kpi_1_success_rate(conn)
        kpi_2_turnaround(conn)
        kpi_3_revisions(conn)
        kpi_4_failure_rate(conn)
        kpi_5_throughput(conn)
        summary(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
