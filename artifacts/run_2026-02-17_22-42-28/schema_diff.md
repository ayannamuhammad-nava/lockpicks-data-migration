# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_fnam** (character)
- **cl_city** (character)
- **cl_zip** (character)
- **cl_ssn** (character)
- **cl_recid** (integer)
- **cl_stat** (character)
- **cl_adr1** (character varying)
- **cl_rgdt** (character)
- **cl_fil1** (character)
- **cl_brtn** (character)
- **cl_lnam** (character)
- **cl_bact** (character)
- **cl_dcsd** (character)
- **cl_phon** (character)
- **cl_dob** (character)
- **cl_emal** (character varying)
- **cl_st** (character)

## New Columns in Modern System

- **address_line1** (character varying)
- **claimant_id** (integer)
- **claimant_status** (character varying)
- **is_deceased** (boolean)
- **date_of_birth** (date)
- **state** (character varying)
- **first_name** (character varying)
- **registered_at** (timestamp without time zone)
- **city** (character varying)
- **last_name** (character varying)
- **ssn_hash** (character varying)
- **zip_code** (character varying)
- **email** (character varying)
- **phone_number** (bigint)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
