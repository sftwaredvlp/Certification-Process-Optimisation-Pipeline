"""
Mock data generator for MID certification analytics.

Creates 4 CSV files simulating 2 years of certification history:
- clients.csv (manufacturer dimension)
- applications.csv (central fact table)  
- certification_results.csv (outcomes)
- audit_results.csv (audit events with failure reasons)

Based on typical distributions from a mid-sized notified body
processing measuring instruments under MID Module B and D.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Fixed seed for reproducibility
RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# --- CONFIG ---
NUM_CLIENTS = 50
NUM_APPLICATIONS = 300
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)

# Client distributions - SMEs dominate but often have less mature QMS
MANUFACTURER_SIZE_DIST = {'SME': 0.65, 'Large': 0.35}

SECTOR_DIST = {
    'Energy': 0.40,
    'Utilities': 0.30,
    'Retail Fuel': 0.15,
    'Transportation': 0.10,
    'Multi-sector': 0.05
}

# Application distributions - meters are the core business (85%)
INSTRUMENT_TYPE_DIST = {
    'Gas Meter': 0.35,
    'Electricity Meter': 0.30,
    'Water Meter': 0.20,
    'Dispenser': 0.10,
    'Taximeter': 0.05
}

MID_MODULE_DIST = {'B': 0.70, 'D': 0.30}
RISK_CLASS_DIST = {'Low': 0.30, 'Medium': 0.50, 'High': 0.20}

# First-time pass rates by (module, manufacturer_size)
# Intentionally low to show improvement opportunity
FIRST_TIME_PASS_RATES = {
    ('B', 'SME'): 0.45,
    ('B', 'Large'): 0.60,
    ('D', 'SME'): 0.55,
    ('D', 'Large'): 0.70
}

BASE_TURNAROUND_DAYS = {'B': 45, 'D': 30}
DAYS_PER_REVISION = 12

# Module B failures: technical file / documentation focused
MODULE_B_FAILURE_REASONS = {
    'Technical file incomplete': 0.25,
    'Documentation inconsistencies': 0.20,
    'Test report gaps': 0.15,
    'Metrological requirements unclear': 0.12,
    'Software documentation missing': 0.10,
    'Durability evidence insufficient': 0.08,
    'Marking/labelling non-compliant': 0.05,
    'EMC test results missing': 0.05
}

# Module D failures: QMS / production process focused
MODULE_D_FAILURE_REASONS = {
    'Training records missing': 0.22,
    'Internal audit gaps': 0.20,
    'Calibration records outdated': 0.15,
    'Non-conformance handling unclear': 0.12,
    'Production process not documented': 0.10,
    'Supplier control insufficient': 0.08,
    'Management review incomplete': 0.08,
    'Corrective action records missing': 0.05
}


def weighted_choice(dist):
    """Random choice weighted by probability distribution."""
    return np.random.choice(list(dist.keys()), p=list(dist.values()))


def random_date(start, end):
    """Random date between start and end."""
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def fmt_client_id(n):
    return f"CLI{n:04d}"

def fmt_app_id(n):
    return f"APP{n:05d}"

def fmt_audit_id(n):
    return f"AUD{n:05d}"


def generate_clients(n):
    """Generate client dimension table."""
    prefixes = ['Euro', 'Global', 'Tech', 'Smart', 'Precision', 'Advanced', 
                'Nordic', 'Central', 'Premier', 'Allied', 'United', 'First',
                'Metro', 'Alpha', 'Delta', 'Sigma', 'Nova', 'Apex']
    suffixes = ['Meters', 'Instruments', 'Systems', 'Technologies', 'Solutions',
                'Metering', 'Devices', 'Engineering', 'Manufacturing', 'Industries']
    
    clients = []
    for i in range(1, n + 1):
        name = f"{random.choice(prefixes)} {random.choice(suffixes)} {'Ltd' if random.random() > 0.5 else 'GmbH'}"
        clients.append({
            'client_id': fmt_client_id(i),
            'company_name': name,
            'manufacturer_size': weighted_choice(MANUFACTURER_SIZE_DIST),
            'sector': weighted_choice(SECTOR_DIST)
        })
    
    print(f"Generated {n} clients")
    return pd.DataFrame(clients)


def generate_applications(n, clients_df):
    """Generate applications fact table."""
    client_sectors = clients_df.set_index('client_id')['sector'].to_dict()
    
    apps = []
    for i in range(1, n + 1):
        client_id = random.choice(clients_df['client_id'].tolist())
        sector = client_sectors[client_id]
        
        # Instrument type influenced by sector
        if sector == 'Energy' and random.random() < 0.8:
            instr = random.choice(['Gas Meter', 'Electricity Meter'])
        elif sector == 'Utilities' and random.random() < 0.7:
            instr = 'Water Meter'
        elif sector == 'Retail Fuel' and random.random() < 0.7:
            instr = 'Dispenser'
        elif sector == 'Transportation' and random.random() < 0.6:
            instr = 'Taximeter'
        else:
            instr = weighted_choice(INSTRUMENT_TYPE_DIST)
        
        apps.append({
            'application_id': fmt_app_id(i),
            'client_id': client_id,
            'submission_date': random_date(START_DATE, END_DATE).strftime('%Y-%m-%d'),
            'instrument_type': instr,
            'mid_module': weighted_choice(MID_MODULE_DIST),
            'risk_class': weighted_choice(RISK_CLASS_DIST)
        })
    
    df = pd.DataFrame(apps).sort_values('submission_date').reset_index(drop=True)
    print(f"Generated {n} applications")
    return df


def generate_cert_results(apps_df, clients_df):
    """Generate certification outcomes."""
    client_sizes = clients_df.set_index('client_id')['manufacturer_size'].to_dict()
    
    results = []
    for _, app in apps_df.iterrows():
        module = app['mid_module']
        mfr_size = client_sizes[app['client_id']]
        submission = datetime.strptime(app['submission_date'], '%Y-%m-%d')
        
        pass_rate = FIRST_TIME_PASS_RATES.get((module, mfr_size), 0.55)
        passed = 1 if random.random() < pass_rate else 0
        
        if passed:
            revisions = 0
        else:
            revisions = np.random.choice([1, 2, 3, 4], p=[0.45, 0.30, 0.15, 0.10])
        
        base_days = BASE_TURNAROUND_DAYS[module]
        total_days = int(base_days + revisions * DAYS_PER_REVISION + random.randint(-10, 10))
        cert_date = submission + timedelta(days=total_days)
        
        # Recent apps might still be pending
        is_recent = submission > END_DATE - timedelta(days=90)
        is_pending = is_recent and random.random() < 0.15
        
        results.append({
            'application_id': app['application_id'],
            'passed_first_time': passed,
            'total_revisions': int(revisions),
            'certification_date': None if is_pending else cert_date.strftime('%Y-%m-%d')
        })
    
    df = pd.DataFrame(results)
    print(f"Generated {len(df)} certification results (pass rate: {df['passed_first_time'].mean()*100:.1f}%)")
    return df


def generate_audit_results(apps_df, cert_df):
    """Generate audit events with module-specific failure reasons."""
    app_modules = apps_df.set_index('application_id')['mid_module'].to_dict()
    app_submissions = apps_df.set_index('application_id')['submission_date'].to_dict()
    cert_dates = cert_df.set_index('application_id')['certification_date'].to_dict()
    revisions = cert_df.set_index('application_id')['total_revisions'].to_dict()
    
    audits = []
    audit_num = 1
    
    for app_id in apps_df['application_id']:
        module = app_modules[app_id]
        submission = datetime.strptime(app_submissions[app_id], '%Y-%m-%d')
        cert_date_str = cert_dates[app_id]
        num_revisions = revisions[app_id]
        
        total_audits = 1 + num_revisions
        
        if cert_date_str:
            cert_date = datetime.strptime(cert_date_str, '%Y-%m-%d')
            span = (cert_date - submission).days
        else:
            span = BASE_TURNAROUND_DAYS[module] + num_revisions * DAYS_PER_REVISION
            cert_date = submission + timedelta(days=span)
        
        days_between = span // total_audits if total_audits > 1 else span
        
        for i in range(total_audits):
            offset = min((i + 1) * days_between, span)
            audit_date = submission + timedelta(days=offset)
            is_final = (i == total_audits - 1)
            
            if is_final and cert_date_str:
                status, reason = 'PASS', None
            elif is_final and not cert_date_str:
                status, reason = 'PENDING', None
            else:
                status = 'FAIL'
                reason = weighted_choice(MODULE_B_FAILURE_REASONS if module == 'B' else MODULE_D_FAILURE_REASONS)
            
            audits.append({
                'audit_id': fmt_audit_id(audit_num),
                'application_id': app_id,
                'audit_date': audit_date.strftime('%Y-%m-%d'),
                'audit_status': status,
                'failure_reason': reason
            })
            audit_num += 1
    
    df = pd.DataFrame(audits)
    fail_count = (df['audit_status'] == 'FAIL').sum()
    print(f"Generated {len(df)} audit events ({fail_count} failures)")
    return df


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(os.path.dirname(script_dir), 'data', 'raw')
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\nOutput: {output_dir}\n")
    
    # Generate in dependency order
    clients = generate_clients(NUM_CLIENTS)
    apps = generate_applications(NUM_APPLICATIONS, clients)
    cert_results = generate_cert_results(apps, clients)
    audit_results = generate_audit_results(apps, cert_results)
    
    # Save CSVs
    clients.to_csv(os.path.join(output_dir, 'clients.csv'), index=False)
    apps.to_csv(os.path.join(output_dir, 'applications.csv'), index=False)
    cert_results.to_csv(os.path.join(output_dir, 'certification_results.csv'), index=False)
    audit_results.to_csv(os.path.join(output_dir, 'audit_results.csv'), index=False)
    
    print(f"\nSaved 4 CSV files to {output_dir}")


if __name__ == "__main__":
    main()
