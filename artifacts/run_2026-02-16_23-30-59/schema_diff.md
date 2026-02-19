# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_phon** (character)
- **cl_lnam** (character)
- **cl_zip** (character)
- **cl_fil1** (character)
- **cl_st** (character)
- **cl_dob** (character)
- **cl_emal** (character varying)
- **cl_city** (character)
- **cl_ssn** (character)
- **cl_rgdt** (character)
- **cl_brtn** (character)
- **cl_dcsd** (character)
- **cl_adr1** (character varying)
- **cl_fnam** (character)
- **cl_stat** (character)
- **cl_recid** (integer)
- **cl_bact** (character)

## New Columns in Modern System

- **phone_number** (bigint)
- **last_name** (character varying)
- **date_of_birth** (date)
- **claimant_status** (character varying)
- **claimant_id** (integer)
- **city** (character varying)
- **state** (character varying)
- **ssn_hash** (character varying)
- **is_deceased** (boolean)
- **address_line1** (character varying)
- **registered_at** (timestamp without time zone)
- **first_name** (character varying)
- **zip_code** (character varying)
- **email** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
