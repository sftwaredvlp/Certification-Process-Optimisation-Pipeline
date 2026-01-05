"""
Data quality validation for MID certification datasets.

Runs 20 checks across 5 categories:
1. Completeness - required fields populated
2. Validity - values in expected ranges
3. Consistency - related records align logically
4. Referential integrity - foreign keys valid
5. Business rules - certification process logic

In certification work, data quality matters because certificates 
are legal documents with regulatory implications.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import re

# Expected patterns and values
CLIENT_ID_PATTERN = r'^CLI\d{4}$'
APP_ID_PATTERN = r'^APP\d{5}$'
AUDIT_ID_PATTERN = r'^AUD\d{5}$'

VALID_MFR_SIZES = ['SME', 'Large']
VALID_SECTORS = ['Energy', 'Utilities', 'Retail Fuel', 'Transportation', 'Multi-sector']
VALID_INSTRUMENTS = ['Gas Meter', 'Electricity Meter', 'Water Meter', 'Dispenser', 'Taximeter']
VALID_MODULES = ['B', 'D']
VALID_RISK = ['Low', 'Medium', 'High']
VALID_STATUS = ['PASS', 'FAIL', 'PENDING']

MAX_REVISIONS = 10
MAX_TURNAROUND = 365
MIN_TURNAROUND = 7


class DataQualityChecker:
    """Runs quality checks on certification data and reports issues."""
    
    def __init__(self, data_dir):
        print("Loading datasets...")
        self.clients = pd.read_csv(os.path.join(data_dir, 'clients.csv'))
        self.apps = pd.read_csv(os.path.join(data_dir, 'applications.csv'))
        self.certs = pd.read_csv(os.path.join(data_dir, 'certification_results.csv'))
        self.audits = pd.read_csv(os.path.join(data_dir, 'audit_results.csv'))
        
        # Parse dates
        self.apps['submission_date'] = pd.to_datetime(self.apps['submission_date'], errors='coerce')
        self.certs['certification_date'] = pd.to_datetime(self.certs['certification_date'], errors='coerce')
        self.audits['audit_date'] = pd.to_datetime(self.audits['audit_date'], errors='coerce')
        
        self.results = []
        print(f"  {len(self.clients)} clients, {len(self.apps)} applications")
        print(f"  {len(self.certs)} cert results, {len(self.audits)} audits\n")
    
    def _add(self, name, passed, issues, severity, details=""):
        self.results.append({
            'name': name, 'passed': passed, 
            'issues': issues, 'severity': severity, 'details': details
        })
    
    # --- 1. COMPLETENESS ---
    
    def check_client_fields(self):
        """All client fields must be populated for valid certificates."""
        missing = self.clients.isna().sum().sum()
        self._add("1.1 Client fields complete", missing == 0, missing, 
                  'CRITICAL' if missing > 0 else 'OK')
    
    def check_app_fields(self):
        """Application fields drive the certification workflow."""
        missing = self.apps.isna().sum().sum()
        self._add("1.2 Application fields complete", missing == 0, missing,
                  'CRITICAL' if missing > 0 else 'OK')
    
    def check_cert_dates(self):
        """Missing cert dates OK only if legitimately pending."""
        missing = self.certs['certification_date'].isna().sum()
        # Get apps with final audit status PENDING
        final_status = self.audits.sort_values('audit_date').groupby('application_id')['audit_status'].last()
        legit_pending = (final_status == 'PENDING').sum()
        unexplained = missing - legit_pending
        
        self._add("1.3 Certification dates", unexplained == 0, missing,
                  'CRITICAL' if unexplained > 0 else 'INFO',
                  f"{legit_pending} legitimately pending")
    
    def check_audit_dates(self):
        """Audit dates required for regulatory audit trail."""
        missing = self.audits['audit_date'].isna().sum()
        self._add("1.4 Audit dates complete", missing == 0, missing,
                  'CRITICAL' if missing > 0 else 'OK')
    
    # --- 2. VALIDITY ---
    
    def check_id_formats(self):
        """Consistent IDs ensure reliable joins."""
        bad_cli = (~self.clients['client_id'].str.match(CLIENT_ID_PATTERN, na=False)).sum()
        bad_app = (~self.apps['application_id'].str.match(APP_ID_PATTERN, na=False)).sum()
        bad_aud = (~self.audits['audit_id'].str.match(AUDIT_ID_PATTERN, na=False)).sum()
        total = bad_cli + bad_app + bad_aud
        self._add("2.1 ID format validity", total == 0, total, 
                  'WARNING' if total > 0 else 'OK')
    
    def check_categorical_values(self):
        """Invalid categories mean wrong requirements applied."""
        issues = 0
        issues += (~self.clients['manufacturer_size'].isin(VALID_MFR_SIZES)).sum()
        issues += (~self.clients['sector'].isin(VALID_SECTORS)).sum()
        issues += (~self.apps['instrument_type'].isin(VALID_INSTRUMENTS)).sum()
        issues += (~self.apps['mid_module'].isin(VALID_MODULES)).sum()
        issues += (~self.apps['risk_class'].isin(VALID_RISK)).sum()
        issues += (~self.audits['audit_status'].isin(VALID_STATUS)).sum()
        self._add("2.2 Categorical values valid", issues == 0, issues,
                  'CRITICAL' if issues > 0 else 'OK')
    
    def check_revision_counts(self):
        """Negative or extreme revisions indicate data errors."""
        neg = (self.certs['total_revisions'] < 0).sum()
        high = (self.certs['total_revisions'] > MAX_REVISIONS).sum()
        total = neg + high
        self._add("2.3 Revision counts valid", total == 0, total,
                  'CRITICAL' if neg > 0 else ('WARNING' if high > 0 else 'OK'))
    
    def check_binary_flags(self):
        """passed_first_time must be 0 or 1."""
        invalid = (~self.certs['passed_first_time'].isin([0, 1])).sum()
        self._add("2.4 Binary flags valid", invalid == 0, invalid,
                  'CRITICAL' if invalid > 0 else 'OK')
    
    # --- 3. CONSISTENCY ---
    
    def check_date_order(self):
        """Certification must occur after submission."""
        merged = self.certs.merge(self.apps[['application_id', 'submission_date']], on='application_id')
        dated = merged[merged['certification_date'].notna()]
        violations = (dated['certification_date'] <= dated['submission_date']).sum()
        self._add("3.1 Date order (submit → cert)", violations == 0, violations,
                  'CRITICAL' if violations > 0 else 'OK')
    
    def check_turnaround_range(self):
        """Turnaround should be 7-365 days."""
        merged = self.certs.merge(self.apps[['application_id', 'submission_date']], on='application_id')
        dated = merged[merged['certification_date'].notna()].copy()
        dated['days'] = (dated['certification_date'] - dated['submission_date']).dt.days
        
        short = (dated['days'] < MIN_TURNAROUND).sum()
        long = (dated['days'] > MAX_TURNAROUND).sum()
        total = short + long
        self._add("3.2 Turnaround range", total == 0, total,
                  'WARNING' if total > 0 else 'OK',
                  f"range: {dated['days'].min()}-{dated['days'].max()} days")
    
    def check_audit_sequence(self):
        """Audits must be chronologically ordered per application."""
        violations = 0
        for app_id, grp in self.audits.groupby('application_id'):
            if not grp['audit_date'].equals(grp['audit_date'].sort_values()):
                violations += 1
        self._add("3.3 Audit date sequence", violations == 0, violations,
                  'WARNING' if violations > 0 else 'OK')
    
    def check_pass_revision_consistency(self):
        """First-time pass must have zero revisions."""
        bad = ((self.certs['passed_first_time'] == 1) & (self.certs['total_revisions'] > 0)).sum()
        self._add("3.4 Pass/revision consistency", bad == 0, bad,
                  'CRITICAL' if bad > 0 else 'OK')
    
    def check_audit_count_consistency(self):
        """Audit count should equal 1 + revisions."""
        counts = self.audits.groupby('application_id').size().reset_index(name='n_audits')
        merged = counts.merge(self.certs[['application_id', 'total_revisions']], on='application_id')
        merged['expected'] = 1 + merged['total_revisions']
        mismatches = (merged['n_audits'] != merged['expected']).sum()
        self._add("3.5 Audit count consistency", mismatches == 0, mismatches,
                  'WARNING' if mismatches > 0 else 'OK')
    
    # --- 4. REFERENTIAL INTEGRITY ---
    
    def check_app_client_ref(self):
        """All applications must reference existing clients."""
        valid = set(self.clients['client_id'])
        orphans = (~self.apps['client_id'].isin(valid)).sum()
        self._add("4.1 App → Client ref", orphans == 0, orphans,
                  'CRITICAL' if orphans > 0 else 'OK')
    
    def check_cert_app_ref(self):
        """All cert results must reference existing applications."""
        valid = set(self.apps['application_id'])
        orphans = (~self.certs['application_id'].isin(valid)).sum()
        self._add("4.2 Cert → App ref", orphans == 0, orphans,
                  'CRITICAL' if orphans > 0 else 'OK')
    
    def check_audit_app_ref(self):
        """All audits must reference existing applications."""
        valid = set(self.apps['application_id'])
        orphans = (~self.audits['application_id'].isin(valid)).sum()
        self._add("4.3 Audit → App ref", orphans == 0, orphans,
                  'CRITICAL' if orphans > 0 else 'OK')
    
    def check_one_to_one_cert(self):
        """Each application needs exactly one certification result."""
        app_ids = set(self.apps['application_id'])
        cert_ids = set(self.certs['application_id'])
        missing = len(app_ids - cert_ids)
        dupes = self.certs.duplicated(subset=['application_id']).sum()
        total = missing + dupes
        self._add("4.4 One-to-one app↔cert", total == 0, total,
                  'CRITICAL' if total > 0 else 'OK')
    
    # --- 5. BUSINESS RULES ---
    
    def check_fail_has_reason(self):
        """Failed audits must document the reason."""
        failed = self.audits[self.audits['audit_status'] == 'FAIL']
        no_reason = failed['failure_reason'].isna().sum()
        self._add("5.1 FAIL has reason", no_reason == 0, no_reason,
                  'CRITICAL' if no_reason > 0 else 'OK')
    
    def check_pass_no_reason(self):
        """Passed audits should not have failure reasons."""
        passed = self.audits[self.audits['audit_status'] == 'PASS']
        has_reason = passed['failure_reason'].notna().sum()
        self._add("5.2 PASS has no reason", has_reason == 0, has_reason,
                  'WARNING' if has_reason > 0 else 'OK')
    
    def check_final_status_alignment(self):
        """Final audit status should align with cert date existence."""
        final = self.audits.sort_values('audit_date').groupby('application_id').last()
        merged = final.merge(self.certs[['application_id', 'certification_date']], on='application_id')
        
        # Certified but not PASS
        cert_not_pass = ((merged['certification_date'].notna()) & (merged['audit_status'] != 'PASS')).sum()
        # PASS but not certified
        pass_no_cert = ((merged['certification_date'].isna()) & (merged['audit_status'] == 'PASS')).sum()
        
        total = cert_not_pass + pass_no_cert
        self._add("5.3 Final audit↔cert alignment", total == 0, total,
                  'CRITICAL' if total > 0 else 'OK')
    
    def run_all(self):
        """Execute all checks."""
        print("Running quality checks...\n")
        
        # Completeness
        self.check_client_fields()
        self.check_app_fields()
        self.check_cert_dates()
        self.check_audit_dates()
        
        # Validity
        self.check_id_formats()
        self.check_categorical_values()
        self.check_revision_counts()
        self.check_binary_flags()
        
        # Consistency
        self.check_date_order()
        self.check_turnaround_range()
        self.check_audit_sequence()
        self.check_pass_revision_consistency()
        self.check_audit_count_consistency()
        
        # Referential
        self.check_app_client_ref()
        self.check_cert_app_ref()
        self.check_audit_app_ref()
        self.check_one_to_one_cert()
        
        # Business rules
        self.check_fail_has_reason()
        self.check_pass_no_reason()
        self.check_final_status_alignment()
        
        return self.results
    
    def print_report(self):
        """Print summary report."""
        icons = {'OK': '✓', 'INFO': 'ℹ', 'WARNING': '⚠', 'CRITICAL': '✗'}
        counts = {'OK': 0, 'INFO': 0, 'WARNING': 0, 'CRITICAL': 0}
        
        print("=" * 60)
        print("DATA QUALITY REPORT")
        print("=" * 60 + "\n")
        
        for r in self.results:
            sev = r['severity']
            counts[sev] += 1
            icon = icons[sev]
            print(f"{icon} [{sev:8}] {r['name']}")
            if r['issues'] > 0:
                print(f"            Issues: {r['issues']}", end="")
                if r['details']:
                    print(f" ({r['details']})", end="")
                print()
        
        print("\n" + "=" * 60)
        print(f"SUMMARY: {len(self.results)} checks")
        print(f"  ✓ OK: {counts['OK']}  ℹ INFO: {counts['INFO']}  ⚠ WARN: {counts['WARNING']}  ✗ CRIT: {counts['CRITICAL']}")
        
        if counts['CRITICAL'] > 0:
            print("\nVERDICT: ✗ Critical issues found")
        elif counts['WARNING'] > 0:
            print("\nVERDICT: ⚠ Acceptable with warnings")
        else:
            print("\nVERDICT: ✓ All checks passed")
        
        return counts


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(script_dir), 'data', 'raw')
    
    checker = DataQualityChecker(data_dir)
    checker.run_all()
    checker.print_report()


if __name__ == "__main__":
    main()
