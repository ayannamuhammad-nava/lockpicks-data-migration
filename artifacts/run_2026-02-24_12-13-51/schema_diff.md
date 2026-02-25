# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_fnam** (character)
- **cl_adr1** (character varying)
- **cl_city** (character)
- **cl_zip** (character)
- **cl_recid** (integer)
- **cl_dob** (character)
- **cl_emal** (character varying)
- **cl_brtn** (character)
- **cl_phon** (character)
- **cl_ssn** (character)
- **cl_rgdt** (character)
- **cl_st** (character)
- **cl_dcsd** (character)
- **cl_stat** (character)
- **cl_fil1** (character)
- **cl_lnam** (character)

## New Columns in Modern System

- **first_name** (character varying)
- **address_line1** (character varying)
- **registered_at** (timestamp without time zone)
- **email** (character varying)
- **claimant_status** (character varying)
- **state** (character varying)
- **zip_code** (character varying)
- **date_of_birth** (date)
- **last_name** (character varying)
- **phone_number** (bigint)
- **is_deceased** (boolean)
- **legacy_system_ref** (character varying)
- **city** (character varying)
- **claimant_id** (integer)
- **ssn_hash** (character varying)

## Type Mismatches

| Column | Legacy Type | Modern Type |
|--------|-------------|-------------|
| cl_bact | character | character varying |

## Summary

- Common columns: 1
- Missing in modern: 16
- New in modern: 15
- Type mismatches: 1
