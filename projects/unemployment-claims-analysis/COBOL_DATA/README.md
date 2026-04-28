# COBOL_DATA — Contact Management System Test Dataset

This folder contains a COBOL-style flat file dataset for verifying that the Lockpicks DM toolkit correctly resolves COBOL abbreviated field names to modern descriptive names.

## Files

| File | Description |
|------|-------------|
| `CONTACTS.cpy` | COBOL copybook defining the record layout (CTMST010.CPY) |
| `CONTACTS.dat` | Pipe-delimited flat file with 15 contact records |
| `create_contacts_legacy.sql` | DDL to create the legacy `contacts` table |
| `load_contacts_legacy.sql` | INSERT statements to load 15 records with intentional issues |
| `create_contacts_modern.sql` | DDL for the modern target `contacts` table |

## Fields (41 COBOL fields)

| COBOL Field | PIC | Expected Modern Name | Type |
|-------------|-----|---------------------|------|
| `ct_recid` | 9(8) | `contact_id` | rename |
| `ct_fnam` | X(25) | `first_name` | rename |
| `ct_mnam` | X(25) | `middle_name` | rename |
| `ct_lnam` | X(30) | `last_name` | rename |
| `ct_sufx` | X(5) | `name_suffix` | rename |
| `ct_ssn` | X(11) | `ssn_hash` | transform (SHA-256) |
| `ct_dob` | X(10) | `date_of_birth` | transform (CHAR->DATE) |
| `ct_gndr` | X(1) | `gender` | rename |
| `ct_ethn` | X(20) | `ethnicity` | rename |
| `ct_ptel` | X(14) | `primary_phone` | transform (CHAR->BIGINT) |
| `ct_mtel` | X(14) | `mobile_phone` | transform (CHAR->BIGINT) |
| `ct_wtel` | X(14) | `work_phone` | transform (CHAR->BIGINT) |
| `ct_emal` | X(60) | `email` | rename |
| `ct_adr1` | X(40) | `address_line1` | rename |
| `ct_adr2` | X(40) | `address_line2` | rename |
| `ct_city` | X(30) | `city` | rename |
| `ct_st` | X(2) | `state` | rename |
| `ct_zip` | X(10) | `zip_code` | rename |
| `ct_adtyp` | X(10) | `address_type` | rename |
| `ct_madr1` | X(40) | `mailing_address_line1` | rename |
| `ct_madr2` | X(40) | `mailing_address_line2` | rename |
| `ct_mcity` | X(30) | `mailing_city` | rename |
| `ct_mst` | X(2) | `mailing_state` | rename |
| `ct_mzip` | X(10) | `mailing_zip_code` | rename |
| `ct_emrg` | X(50) | `emergency_contact_name` | rename |
| `ct_etel` | X(14) | `emergency_contact_phone` | transform (CHAR->BIGINT) |
| `ct_erel` | X(20) | `emergency_contact_relation` | rename |
| `ct_dln` | X(20) | `drivers_license_number` | rename |
| `ct_dlst` | X(2) | `drivers_license_state` | rename |
| `ct_bact` | X(20) | *(archived)* | PCI-DSS compliance |
| `ct_brtn` | X(20) | *(archived)* | PCI-DSS compliance |
| `ct_mstat` | X(10) | `marital_status` | rename |
| `ct_dpnds` | 9(2) | `dependents_count` | rename |
| `ct_lang` | X(10) | `language_preference` | rename |
| `ct_vetf` | X(1) | `is_veteran` | transform (Y/N->BOOLEAN) |
| `ct_disf` | X(1) | `is_disabled` | transform (Y/N->BOOLEAN) |
| `ct_stat` | X(10) | `contact_status` | rename |
| `ct_crtdt` | X(26) | `created_at` | transform (CHAR->TIMESTAMPTZ) |
| `ct_upddt` | X(26) | `updated_at` | transform (CHAR->TIMESTAMPTZ) |
| `ct_srccd` | X(10) | `source_code` | rename |
| `ct_fil1` | X(50) | *(removed)* | COBOL FILLER |
| `ct_fil2` | X(30) | *(removed)* | COBOL FILLER |

## Intentional Data Quality Issues (12)

1. **Duplicate SSN** — ct_recid 1 and 4 share `445-67-8901`
2. **NULL emails** — ct_recid 3, 8, 13
3. **PII in plaintext** — `ct_ssn`, `ct_bact`, `ct_brtn`, `ct_dln`
4. **Mixed date formats** — ISO (`1985-03-22`), US (`06/15/1978`), text (`12-Mar-1990`), compact (`19920620`), slash (`1988/04/17`), ambiguous (`April 5 93`)
5. **Inconsistent phone formats** — dashes, parentheses, dots, spaces, plain digits
6. **Status inconsistencies** — ACTIVE, active, Active, ACT, INACTV, SUSPND, DECD
7. **Leading whitespace** — ct_recid 8 first name has leading spaces
8. **Deceased with bank info** — ct_recid 13 is DECD but bank fields populated
9. **Missing mailing address** — ct_recid 8 is BUSNS type but has mailing (OK); ct_recid 4 HOME with no mailing
10. **Missing driver's license** — ct_recid 7
11. **Marital status case mismatch** — `married` vs `Married`
12. **Empty bank fields** — ct_recid 8 has no bank account or routing number

## Usage

```bash
# Load into PostgreSQL
docker exec -e PGPASSWORD=secret123 <container> psql -U app -d legacy_db -f /tmp/create_contacts_legacy.sql
docker exec -e PGPASSWORD=secret123 <container> psql -U app -d legacy_db -f /tmp/load_contacts_legacy.sql
docker exec -e PGPASSWORD=secret123 <container> psql -U app -d modern_db -f /tmp/create_contacts_modern.sql
```

Then run the full DM pipeline to verify COBOL field name resolution:
```bash
dm discover --enrich -p projects/unemployment-claims-analysis
dm validate --phase pre --dataset contacts -p projects/unemployment-claims-analysis
```
