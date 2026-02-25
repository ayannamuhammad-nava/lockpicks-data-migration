# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_fnam** (character)
- **cl_rgdt** (character)
- **cl_st** (character)
- **cl_dob** (character)
- **cl_fil1** (character)
- **cl_lnam** (character)
- **cl_adr1** (character varying)
- **cl_zip** (character)
- **cl_city** (character)
- **cl_stat** (character)
- **cl_ssn** (character)
- **cl_emal** (character varying)
- **cl_brtn** (character)
- **cl_recid** (integer)
- **cl_phon** (character)
- **cl_dcsd** (character)

## New Columns in Modern System

- **date_of_birth** (date)
- **registered_at** (timestamp without time zone)
- **claimant_status** (character varying)
- **address_line1** (character varying)
- **legacy_system_ref** (character varying)
- **last_name** (character varying)
- **is_deceased** (boolean)
- **claimant_id** (integer)
- **zip_code** (character varying)
- **first_name** (character varying)
- **state** (character varying)
- **ssn_hash** (character varying)
- **email** (character varying)
- **city** (character varying)
- **phone_number** (bigint)

## Type Mismatches

| Column | Legacy Type | Modern Type |
|--------|-------------|-------------|
| cl_bact | character | character varying |

## Summary

- Common columns: 1
- Missing in modern: 16
- New in modern: 15
- Type mismatches: 1
