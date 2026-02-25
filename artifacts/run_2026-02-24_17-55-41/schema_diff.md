# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_stat** (character)
- **cl_lnam** (character)
- **cl_phon** (character)
- **cl_city** (character)
- **cl_rgdt** (character)
- **cl_recid** (integer)
- **cl_emal** (character varying)
- **cl_fnam** (character)
- **cl_fil1** (character)
- **cl_ssn** (character)
- **cl_st** (character)
- **cl_brtn** (character)
- **cl_dob** (character)
- **cl_dcsd** (character)
- **cl_adr1** (character varying)
- **cl_zip** (character)

## New Columns in Modern System

- **ssn_hash** (character varying)
- **legacy_system_ref** (character varying)
- **claimant_status** (character varying)
- **phone_number** (bigint)
- **email** (character varying)
- **state** (character varying)
- **last_name** (character varying)
- **address_line1** (character varying)
- **registered_at** (timestamp without time zone)
- **is_deceased** (boolean)
- **claimant_id** (integer)
- **zip_code** (character varying)
- **first_name** (character varying)
- **city** (character varying)
- **date_of_birth** (date)

## Type Mismatches

| Column | Legacy Type | Modern Type |
|--------|-------------|-------------|
| cl_bact | character | character varying |

## Summary

- Common columns: 1
- Missing in modern: 16
- New in modern: 15
- Type mismatches: 1
