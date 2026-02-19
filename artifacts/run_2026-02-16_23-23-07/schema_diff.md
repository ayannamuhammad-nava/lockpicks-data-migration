# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_phon** (character)
- **cl_zip** (character)
- **cl_lnam** (character)
- **cl_city** (character)
- **cl_bact** (character)
- **cl_stat** (character)
- **cl_rgdt** (character)
- **cl_recid** (integer)
- **cl_dob** (character)
- **cl_ssn** (character)
- **cl_emal** (character varying)
- **cl_fnam** (character)
- **cl_st** (character)
- **cl_dcsd** (character)
- **cl_fil1** (character)
- **cl_brtn** (character)
- **cl_adr1** (character varying)

## New Columns in Modern System

- **first_name** (character varying)
- **address_line1** (character varying)
- **phone_number** (bigint)
- **zip_code** (character varying)
- **claimant_status** (character varying)
- **date_of_birth** (date)
- **email** (character varying)
- **is_deceased** (boolean)
- **ssn_hash** (character varying)
- **city** (character varying)
- **registered_at** (timestamp without time zone)
- **last_name** (character varying)
- **claimant_id** (integer)
- **state** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
