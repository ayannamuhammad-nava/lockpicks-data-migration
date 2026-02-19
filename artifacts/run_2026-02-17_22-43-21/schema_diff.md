# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_ssn** (character)
- **cl_city** (character)
- **cl_fnam** (character)
- **cl_bact** (character)
- **cl_emal** (character varying)
- **cl_st** (character)
- **cl_adr1** (character varying)
- **cl_brtn** (character)
- **cl_recid** (integer)
- **cl_lnam** (character)
- **cl_phon** (character)
- **cl_fil1** (character)
- **cl_rgdt** (character)
- **cl_zip** (character)
- **cl_dcsd** (character)
- **cl_dob** (character)
- **cl_stat** (character)

## New Columns in Modern System

- **date_of_birth** (date)
- **state** (character varying)
- **is_deceased** (boolean)
- **city** (character varying)
- **first_name** (character varying)
- **zip_code** (character varying)
- **phone_number** (bigint)
- **last_name** (character varying)
- **claimant_id** (integer)
- **claimant_status** (character varying)
- **email** (character varying)
- **address_line1** (character varying)
- **ssn_hash** (character varying)
- **registered_at** (timestamp without time zone)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
