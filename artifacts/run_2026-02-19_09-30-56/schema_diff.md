# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_dob** (character)
- **cl_stat** (character)
- **cl_fnam** (character)
- **cl_bact** (character)
- **cl_emal** (character varying)
- **cl_lnam** (character)
- **cl_rgdt** (character)
- **cl_st** (character)
- **cl_ssn** (character)
- **cl_city** (character)
- **cl_zip** (character)
- **cl_fil1** (character)
- **cl_phon** (character)
- **cl_recid** (integer)
- **cl_adr1** (character varying)
- **cl_dcsd** (character)
- **cl_brtn** (character)

## New Columns in Modern System

- **registered_at** (timestamp without time zone)
- **phone_number** (bigint)
- **ssn_hash** (character varying)
- **claimant_id** (integer)
- **date_of_birth** (date)
- **city** (character varying)
- **first_name** (character varying)
- **address_line1** (character varying)
- **claimant_status** (character varying)
- **last_name** (character varying)
- **email** (character varying)
- **zip_code** (character varying)
- **state** (character varying)
- **is_deceased** (boolean)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
