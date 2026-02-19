# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_st** (character)
- **cl_phon** (character)
- **cl_ssn** (character)
- **cl_rgdt** (character)
- **cl_zip** (character)
- **cl_bact** (character)
- **cl_stat** (character)
- **cl_emal** (character varying)
- **cl_lnam** (character)
- **cl_city** (character)
- **cl_fil1** (character)
- **cl_dcsd** (character)
- **cl_dob** (character)
- **cl_brtn** (character)
- **cl_fnam** (character)
- **cl_adr1** (character varying)
- **cl_recid** (integer)

## New Columns in Modern System

- **email** (character varying)
- **address_line1** (character varying)
- **first_name** (character varying)
- **state** (character varying)
- **claimant_status** (character varying)
- **zip_code** (character varying)
- **claimant_id** (integer)
- **ssn_hash** (character varying)
- **last_name** (character varying)
- **date_of_birth** (date)
- **city** (character varying)
- **registered_at** (timestamp without time zone)
- **is_deceased** (boolean)
- **phone_number** (bigint)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
