# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_zip** (character)
- **cl_dob** (character)
- **cl_adr1** (character varying)
- **cl_phon** (character)
- **cl_bact** (character)
- **cl_brtn** (character)
- **cl_rgdt** (character)
- **cl_dcsd** (character)
- **cl_lnam** (character)
- **cl_ssn** (character)
- **cl_recid** (integer)
- **cl_stat** (character)
- **cl_st** (character)
- **cl_fil1** (character)
- **cl_city** (character)
- **cl_fnam** (character)
- **cl_emal** (character varying)

## New Columns in Modern System

- **is_deceased** (boolean)
- **zip_code** (character varying)
- **state** (character varying)
- **claimant_status** (character varying)
- **last_name** (character varying)
- **address_line1** (character varying)
- **date_of_birth** (date)
- **city** (character varying)
- **registered_at** (timestamp without time zone)
- **phone_number** (bigint)
- **claimant_id** (integer)
- **first_name** (character varying)
- **email** (character varying)
- **ssn_hash** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
