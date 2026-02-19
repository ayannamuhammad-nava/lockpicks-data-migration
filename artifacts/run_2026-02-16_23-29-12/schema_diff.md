# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_zip** (character)
- **cl_dcsd** (character)
- **cl_phon** (character)
- **cl_bact** (character)
- **cl_brtn** (character)
- **cl_emal** (character varying)
- **cl_dob** (character)
- **cl_st** (character)
- **cl_fnam** (character)
- **cl_lnam** (character)
- **cl_city** (character)
- **cl_adr1** (character varying)
- **cl_stat** (character)
- **cl_fil1** (character)
- **cl_ssn** (character)
- **cl_recid** (integer)
- **cl_rgdt** (character)

## New Columns in Modern System

- **date_of_birth** (date)
- **address_line1** (character varying)
- **email** (character varying)
- **zip_code** (character varying)
- **city** (character varying)
- **state** (character varying)
- **claimant_id** (integer)
- **last_name** (character varying)
- **ssn_hash** (character varying)
- **first_name** (character varying)
- **claimant_status** (character varying)
- **is_deceased** (boolean)
- **registered_at** (timestamp without time zone)
- **phone_number** (bigint)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
