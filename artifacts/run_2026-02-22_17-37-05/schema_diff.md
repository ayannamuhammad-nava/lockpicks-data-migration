# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_phon** (character)
- **cl_dcsd** (character)
- **cl_brtn** (character)
- **cl_lnam** (character)
- **cl_adr1** (character varying)
- **cl_st** (character)
- **cl_emal** (character varying)
- **cl_recid** (integer)
- **cl_city** (character)
- **cl_ssn** (character)
- **cl_zip** (character)
- **cl_stat** (character)
- **cl_rgdt** (character)
- **cl_fil1** (character)
- **cl_fnam** (character)
- **cl_dob** (character)

## New Columns in Modern System

- **phone_number** (bigint)
- **state** (character varying)
- **registered_at** (timestamp without time zone)
- **address_line1** (character varying)
- **legacy_system_ref** (character varying)
- **city** (character varying)
- **date_of_birth** (date)
- **last_name** (character varying)
- **claimant_id** (integer)
- **claimant_status** (character varying)
- **is_deceased** (boolean)
- **zip_code** (character varying)
- **ssn_hash** (character varying)
- **first_name** (character varying)
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
