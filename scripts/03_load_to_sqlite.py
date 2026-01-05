"""
Load CSV data into SQLite database with proper schema and analysis views.
"""

import sqlite3
import pandas as pd
import os


def get_paths():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(project_dir, 'data', 'raw')
    db_path = os.path.join(project_dir, 'data', 'certification.db')
    return data_dir, db_path


def create_schema(conn):
    """Create tables with constraints and indexes."""
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")
    
    # Drop in reverse dependency order
    for t in ['audit_results', 'certification_results', 'applications', 'clients']:
        cur.execute(f"DROP TABLE IF EXISTS {t}")
    
    cur.execute("""
        CREATE TABLE clients (
            client_id TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            manufacturer_size TEXT NOT NULL CHECK(manufacturer_size IN ('SME', 'Large')),
            sector TEXT NOT NULL
        )
    """)
    
    cur.execute("""
        CREATE TABLE applications (
            application_id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            submission_date TEXT NOT NULL,
            instrument_type TEXT NOT NULL,
            mid_module TEXT NOT NULL CHECK(mid_module IN ('B', 'D')),
            risk_class TEXT NOT NULL CHECK(risk_class IN ('Low', 'Medium', 'High')),
            FOREIGN KEY (client_id) REFERENCES clients(client_id)
        )
    """)
    
    cur.execute("""
        CREATE TABLE certification_results (
            application_id TEXT PRIMARY KEY,
            passed_first_time INTEGER NOT NULL CHECK(passed_first_time IN (0, 1)),
            total_revisions INTEGER NOT NULL CHECK(total_revisions >= 0),
            certification_date TEXT,
            FOREIGN KEY (application_id) REFERENCES applications(application_id)
        )
    """)
    
    cur.execute("""
        CREATE TABLE audit_results (
            audit_id TEXT PRIMARY KEY,
            application_id TEXT NOT NULL,
            audit_date TEXT NOT NULL,
            audit_status TEXT NOT NULL CHECK(audit_status IN ('PASS', 'FAIL', 'PENDING')),
            failure_reason TEXT,
            FOREIGN KEY (application_id) REFERENCES applications(application_id)
        )
    """)
    
    # Indexes for common joins and filters
    cur.execute("CREATE INDEX idx_app_client ON applications(client_id)")
    cur.execute("CREATE INDEX idx_app_module ON applications(mid_module)")
    cur.execute("CREATE INDEX idx_app_instrument ON applications(instrument_type)")
    cur.execute("CREATE INDEX idx_audit_app ON audit_results(application_id)")
    cur.execute("CREATE INDEX idx_audit_status ON audit_results(audit_status)")
    cur.execute("CREATE INDEX idx_app_date ON applications(submission_date)")
    cur.execute("CREATE INDEX idx_cert_date ON certification_results(certification_date)")
    
    conn.commit()
    print("Created 4 tables with 7 indexes")


def load_data(conn, data_dir):
    """Load CSVs in FK-safe order."""
    tables = [
        ('clients', 'clients.csv'),
        ('applications', 'applications.csv'),
        ('certification_results', 'certification_results.csv'),
        ('audit_results', 'audit_results.csv')
    ]
    
    for table, csv_file in tables:
        df = pd.read_csv(os.path.join(data_dir, csv_file))
        df.to_sql(table, conn, if_exists='append', index=False)
        print(f"  {table}: {len(df)} rows")


def create_views(conn):
    """Create analysis views for reporting."""
    cur = conn.cursor()
    
    # Full application details with turnaround calculation
    cur.execute("""
        CREATE VIEW IF NOT EXISTS v_application_details AS
        SELECT 
            a.application_id,
            a.submission_date,
            a.instrument_type,
            a.mid_module,
            a.risk_class,
            c.client_id,
            c.company_name,
            c.manufacturer_size,
            c.sector,
            cr.passed_first_time,
            cr.total_revisions,
            cr.certification_date,
            CAST(julianday(cr.certification_date) - julianday(a.submission_date) AS INTEGER) AS turnaround_days
        FROM applications a
        JOIN clients c ON a.client_id = c.client_id
        JOIN certification_results cr ON a.application_id = cr.application_id
    """)
    
    # Failed audits with context
    cur.execute("""
        CREATE VIEW IF NOT EXISTS v_failures AS
        SELECT 
            ar.audit_id,
            ar.application_id,
            ar.audit_date,
            ar.failure_reason,
            a.mid_module,
            a.instrument_type,
            a.risk_class,
            c.manufacturer_size
        FROM audit_results ar
        JOIN applications a ON ar.application_id = a.application_id
        JOIN clients c ON a.client_id = c.client_id
        WHERE ar.audit_status = 'FAIL'
    """)
    
    # Monthly throughput
    cur.execute("""
        CREATE VIEW IF NOT EXISTS v_monthly_throughput AS
        SELECT 
            strftime('%Y-%m', certification_date) AS month,
            COUNT(*) AS certifications,
            SUM(passed_first_time) AS first_time_passes,
            ROUND(AVG(passed_first_time) * 100, 1) AS success_rate
        FROM certification_results
        WHERE certification_date IS NOT NULL
        GROUP BY strftime('%Y-%m', certification_date)
    """)
    
    # Module B vs D comparison
    cur.execute("""
        CREATE VIEW IF NOT EXISTS v_module_comparison AS
        SELECT 
            a.mid_module,
            COUNT(*) AS applications,
            SUM(cr.passed_first_time) AS first_time_passes,
            ROUND(AVG(cr.passed_first_time) * 100, 1) AS success_rate,
            ROUND(AVG(cr.total_revisions), 2) AS avg_revisions,
            ROUND(AVG(julianday(cr.certification_date) - julianday(a.submission_date)), 1) AS avg_days
        FROM applications a
        JOIN certification_results cr ON a.application_id = cr.application_id
        WHERE cr.certification_date IS NOT NULL
        GROUP BY a.mid_module
    """)
    
    # Client performance
    cur.execute("""
        CREATE VIEW IF NOT EXISTS v_client_performance AS
        SELECT 
            c.client_id,
            c.company_name,
            c.manufacturer_size,
            c.sector,
            COUNT(*) AS applications,
            SUM(cr.passed_first_time) AS first_time_passes,
            ROUND(AVG(cr.passed_first_time) * 100, 1) AS success_rate,
            SUM(cr.total_revisions) AS total_revisions
        FROM clients c
        JOIN applications a ON c.client_id = a.client_id
        JOIN certification_results cr ON a.application_id = cr.application_id
        GROUP BY c.client_id
    """)
    
    conn.commit()
    print("Created 5 views")


def verify(conn):
    """Quick sanity check."""
    cur = conn.cursor()
    
    print("\nVerification:")
    cur.execute("SELECT * FROM v_module_comparison")
    for row in cur.fetchall():
        print(f"  Module {row[0]}: {row[1]} apps, {row[3]}% pass rate, {row[5]} days avg")
    
    cur.execute("SELECT failure_reason, COUNT(*) n FROM v_failures GROUP BY failure_reason ORDER BY n DESC LIMIT 3")
    print("  Top failures:", [r[0] for r in cur.fetchall()])


def main():
    data_dir, db_path = get_paths()
    
    if os.path.exists(db_path):
        os.remove(db_path)
    
    print(f"Database: {db_path}\n")
    conn = sqlite3.connect(db_path)
    
    try:
        create_schema(conn)
        load_data(conn, data_dir)
        create_views(conn)
        verify(conn)
        print(f"\nDone. Size: {os.path.getsize(db_path) / 1024:.0f} KB")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
