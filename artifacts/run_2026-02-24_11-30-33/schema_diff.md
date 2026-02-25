# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_zip** (character)
- **cl_dcsd** (character)
- **cl_adr1** (character varying)
- **cl_emal** (character varying)
- **cl_recid** (integer)
- **cl_rgdt** (character)
- **cl_brtn** (character)
- **cl_lnam** (character)
- **cl_fnam** (character)
- **cl_phon** (character)
- **cl_fil1** (character)
- **cl_dob** (character)
- **cl_city** (character)
- **cl_st** (character)
- **cl_stat** (character)
- **cl_ssn** (character)

## New Columns in Modern System

- **state** (character varying)
- **legacy_system_ref** (character varying)
- **city** (character varying)
- **claimant_id** (integer)
- **zip_code** (character varying)
- **phone_number** (bigint)
- **email** (character varying)
- **ssn_hash** (character varying)
- **last_name** (character varying)
- **is_deceased** (boolean)
- **date_of_birth** (date)
- **claimant_status** (character varying)
- **registered_at** (timestamp without time zone)
- **address_line1** (character varying)
- **first_name** (character varying)

## Type Mismatches

| Column | Legacy Type | Modern Type |
|--------|-------------|-------------|
| cl_bact | character | character varying |

## Summary

- Common columns: 1
- Missing in modern: 16
- New in modern: 15
- Type mismatches: 1
