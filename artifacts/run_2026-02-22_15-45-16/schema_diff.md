# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_recid** (integer)
- **cl_ssn** (character)
- **cl_rgdt** (character)
- **cl_fil1** (character)
- **cl_adr1** (character varying)
- **cl_zip** (character)
- **cl_brtn** (character)
- **cl_bact** (character)
- **cl_emal** (character varying)
- **cl_lnam** (character)
- **cl_fnam** (character)
- **cl_stat** (character)
- **cl_city** (character)
- **cl_dcsd** (character)
- **cl_dob** (character)
- **cl_st** (character)
- **cl_phon** (character)

## New Columns in Modern System

- **ssn_hash** (character varying)
- **claimant_status** (character varying)
- **address_line1** (character varying)
- **registered_at** (timestamp without time zone)
- **email** (character varying)
- **zip_code** (character varying)
- **phone_number** (bigint)
- **first_name** (character varying)
- **last_name** (character varying)
- **state** (character varying)
- **date_of_birth** (date)
- **is_deceased** (boolean)
- **claimant_id** (integer)
- **city** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
