# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_dcsd** (character)
- **cl_recid** (integer)
- **cl_fnam** (character)
- **cl_fil1** (character)
- **cl_adr1** (character varying)
- **cl_brtn** (character)
- **cl_st** (character)
- **cl_dob** (character)
- **cl_ssn** (character)
- **cl_lnam** (character)
- **cl_emal** (character varying)
- **cl_zip** (character)
- **cl_city** (character)
- **cl_stat** (character)
- **cl_phon** (character)
- **cl_rgdt** (character)
- **cl_bact** (character)

## New Columns in Modern System

- **phone_number** (bigint)
- **zip_code** (character varying)
- **last_name** (character varying)
- **ssn_hash** (character varying)
- **address_line1** (character varying)
- **first_name** (character varying)
- **city** (character varying)
- **state** (character varying)
- **registered_at** (timestamp without time zone)
- **claimant_status** (character varying)
- **is_deceased** (boolean)
- **email** (character varying)
- **claimant_id** (integer)
- **date_of_birth** (date)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
