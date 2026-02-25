# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_st** (character)
- **cl_dob** (character)
- **cl_fnam** (character)
- **cl_emal** (character varying)
- **cl_city** (character)
- **cl_fil1** (character)
- **cl_adr1** (character varying)
- **cl_brtn** (character)
- **cl_dcsd** (character)
- **cl_recid** (integer)
- **cl_stat** (character)
- **cl_rgdt** (character)
- **cl_ssn** (character)
- **cl_lnam** (character)
- **cl_zip** (character)
- **cl_bact** (character)
- **cl_phon** (character)

## New Columns in Modern System

- **registered_at** (timestamp without time zone)
- **last_name** (character varying)
- **date_of_birth** (date)
- **is_deceased** (boolean)
- **email** (character varying)
- **first_name** (character varying)
- **address_line1** (character varying)
- **claimant_id** (integer)
- **state** (character varying)
- **city** (character varying)
- **zip_code** (character varying)
- **claimant_status** (character varying)
- **phone_number** (bigint)
- **ssn_hash** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
