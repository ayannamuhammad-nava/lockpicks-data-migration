# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_city** (character)
- **cl_adr1** (character varying)
- **cl_fnam** (character)
- **cl_phon** (character)
- **cl_rgdt** (character)
- **cl_dob** (character)
- **cl_lnam** (character)
- **cl_ssn** (character)
- **cl_zip** (character)
- **cl_bact** (character)
- **cl_fil1** (character)
- **cl_brtn** (character)
- **cl_stat** (character)
- **cl_dcsd** (character)
- **cl_st** (character)
- **cl_recid** (integer)
- **cl_emal** (character varying)

## New Columns in Modern System

- **registered_at** (timestamp without time zone)
- **claimant_status** (character varying)
- **state** (character varying)
- **ssn_hash** (character varying)
- **email** (character varying)
- **address_line1** (character varying)
- **city** (character varying)
- **is_deceased** (boolean)
- **first_name** (character varying)
- **zip_code** (character varying)
- **claimant_id** (integer)
- **phone_number** (bigint)
- **date_of_birth** (date)
- **last_name** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
