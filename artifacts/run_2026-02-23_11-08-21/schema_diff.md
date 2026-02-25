# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_rgdt** (character)
- **cl_ssn** (character)
- **cl_dcsd** (character)
- **cl_lnam** (character)
- **cl_fil1** (character)
- **cl_fnam** (character)
- **cl_adr1** (character varying)
- **cl_city** (character)
- **cl_stat** (character)
- **cl_brtn** (character)
- **cl_phon** (character)
- **cl_zip** (character)
- **cl_recid** (integer)
- **cl_emal** (character varying)
- **cl_st** (character)
- **cl_dob** (character)

## New Columns in Modern System

- **phone_number** (bigint)
- **email** (character varying)
- **registered_at** (timestamp without time zone)
- **claimant_status** (character varying)
- **state** (character varying)
- **ssn_hash** (character varying)
- **date_of_birth** (date)
- **last_name** (character varying)
- **legacy_system_ref** (character varying)
- **address_line1** (character varying)
- **first_name** (character varying)
- **city** (character varying)
- **zip_code** (character varying)
- **claimant_id** (integer)
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
