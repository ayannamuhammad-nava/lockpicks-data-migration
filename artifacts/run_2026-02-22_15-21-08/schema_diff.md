# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_st** (character)
- **cl_city** (character)
- **cl_dob** (character)
- **cl_stat** (character)
- **cl_recid** (integer)
- **cl_rgdt** (character)
- **cl_zip** (character)
- **cl_ssn** (character)
- **cl_adr1** (character varying)
- **cl_brtn** (character)
- **cl_fil1** (character)
- **cl_fnam** (character)
- **cl_bact** (character)
- **cl_dcsd** (character)
- **cl_phon** (character)
- **cl_lnam** (character)
- **cl_emal** (character varying)

## New Columns in Modern System

- **claimant_status** (character varying)
- **is_deceased** (boolean)
- **registered_at** (timestamp without time zone)
- **phone_number** (bigint)
- **zip_code** (character varying)
- **city** (character varying)
- **last_name** (character varying)
- **address_line1** (character varying)
- **email** (character varying)
- **claimant_id** (integer)
- **first_name** (character varying)
- **ssn_hash** (character varying)
- **state** (character varying)
- **date_of_birth** (date)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
