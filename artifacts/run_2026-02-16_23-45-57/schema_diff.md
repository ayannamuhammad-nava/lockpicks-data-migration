# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_ssn** (character)
- **cl_city** (character)
- **cl_lnam** (character)
- **cl_rgdt** (character)
- **cl_dob** (character)
- **cl_zip** (character)
- **cl_bact** (character)
- **cl_phon** (character)
- **cl_dcsd** (character)
- **cl_fnam** (character)
- **cl_emal** (character varying)
- **cl_brtn** (character)
- **cl_adr1** (character varying)
- **cl_st** (character)
- **cl_fil1** (character)
- **cl_recid** (integer)
- **cl_stat** (character)

## New Columns in Modern System

- **date_of_birth** (date)
- **phone_number** (bigint)
- **last_name** (character varying)
- **state** (character varying)
- **city** (character varying)
- **zip_code** (character varying)
- **claimant_id** (integer)
- **email** (character varying)
- **registered_at** (timestamp without time zone)
- **is_deceased** (boolean)
- **ssn_hash** (character varying)
- **claimant_status** (character varying)
- **address_line1** (character varying)
- **first_name** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
