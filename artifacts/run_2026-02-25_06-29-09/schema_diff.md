# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_phon** (character)
- **cl_st** (character)
- **cl_recid** (integer)
- **cl_dob** (character)
- **cl_fnam** (character)
- **cl_ssn** (character)
- **cl_zip** (character)
- **cl_lnam** (character)
- **cl_stat** (character)
- **cl_adr1** (character varying)
- **cl_rgdt** (character)
- **cl_emal** (character varying)
- **cl_dcsd** (character)
- **cl_fil1** (character)
- **cl_city** (character)
- **cl_brtn** (character)

## New Columns in Modern System

- **last_name** (character varying)
- **state** (character varying)
- **claimant_status** (character varying)
- **city** (character varying)
- **claimant_id** (integer)
- **zip_code** (character varying)
- **ssn_hash** (character varying)
- **date_of_birth** (date)
- **legacy_system_ref** (character varying)
- **phone_number** (bigint)
- **registered_at** (timestamp without time zone)
- **email** (character varying)
- **is_deceased** (boolean)
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
