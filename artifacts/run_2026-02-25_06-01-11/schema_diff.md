# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_adr1** (character varying)
- **cl_lnam** (character)
- **cl_fnam** (character)
- **cl_emal** (character varying)
- **cl_brtn** (character)
- **cl_phon** (character)
- **cl_recid** (integer)
- **cl_zip** (character)
- **cl_stat** (character)
- **cl_ssn** (character)
- **cl_st** (character)
- **cl_dcsd** (character)
- **cl_city** (character)
- **cl_rgdt** (character)
- **cl_dob** (character)
- **cl_fil1** (character)

## New Columns in Modern System

- **ssn_hash** (character varying)
- **date_of_birth** (date)
- **zip_code** (character varying)
- **legacy_system_ref** (character varying)
- **last_name** (character varying)
- **email** (character varying)
- **first_name** (character varying)
- **city** (character varying)
- **claimant_id** (integer)
- **state** (character varying)
- **registered_at** (timestamp without time zone)
- **phone_number** (bigint)
- **address_line1** (character varying)
- **claimant_status** (character varying)
- **is_deceased** (boolean)

## Type Mismatches

| Column | Legacy Type | Modern Type |
|--------|-------------|-------------|
| cl_bact | character | character varying |

## Summary

- Common columns: 1
- Missing in modern: 16
- New in modern: 15
- Type mismatches: 1
