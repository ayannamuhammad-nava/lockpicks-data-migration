# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_zip** (character)
- **cl_dcsd** (character)
- **cl_rgdt** (character)
- **cl_phon** (character)
- **cl_emal** (character varying)
- **cl_fil1** (character)
- **cl_adr1** (character varying)
- **cl_ssn** (character)
- **cl_brtn** (character)
- **cl_fnam** (character)
- **cl_recid** (integer)
- **cl_dob** (character)
- **cl_stat** (character)
- **cl_city** (character)
- **cl_lnam** (character)
- **cl_st** (character)

## New Columns in Modern System

- **registered_at** (timestamp without time zone)
- **date_of_birth** (date)
- **address_line1** (character varying)
- **ssn_hash** (character varying)
- **claimant_status** (character varying)
- **state** (character varying)
- **last_name** (character varying)
- **claimant_id** (integer)
- **legacy_system_ref** (character varying)
- **is_deceased** (boolean)
- **phone_number** (bigint)
- **zip_code** (character varying)
- **first_name** (character varying)
- **city** (character varying)
- **email** (character varying)

## Type Mismatches

| Column | Legacy Type | Modern Type |
|--------|-------------|-------------|
| cl_bact | character | character varying |

## Summary

- Common columns: 1
- Missing in modern: 16
- New in modern: 15
- Type mismatches: 1
