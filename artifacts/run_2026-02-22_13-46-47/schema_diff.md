# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_st** (character)
- **cl_adr1** (character varying)
- **cl_rgdt** (character)
- **cl_emal** (character varying)
- **cl_zip** (character)
- **cl_lnam** (character)
- **cl_stat** (character)
- **cl_dcsd** (character)
- **cl_fnam** (character)
- **cl_ssn** (character)
- **cl_city** (character)
- **cl_recid** (integer)
- **cl_fil1** (character)
- **cl_phon** (character)
- **cl_brtn** (character)
- **cl_bact** (character)
- **cl_dob** (character)

## New Columns in Modern System

- **phone_number** (bigint)
- **ssn_hash** (character varying)
- **last_name** (character varying)
- **registered_at** (timestamp without time zone)
- **is_deceased** (boolean)
- **claimant_id** (integer)
- **city** (character varying)
- **first_name** (character varying)
- **address_line1** (character varying)
- **date_of_birth** (date)
- **state** (character varying)
- **zip_code** (character varying)
- **email** (character varying)
- **claimant_status** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
