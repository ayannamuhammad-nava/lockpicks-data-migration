# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_adr1** (character varying)
- **cl_fnam** (character)
- **cl_st** (character)
- **cl_lnam** (character)
- **cl_dcsd** (character)
- **cl_ssn** (character)
- **cl_recid** (integer)
- **cl_city** (character)
- **cl_phon** (character)
- **cl_emal** (character varying)
- **cl_rgdt** (character)
- **cl_stat** (character)
- **cl_dob** (character)
- **cl_zip** (character)
- **cl_brtn** (character)
- **cl_fil1** (character)
- **cl_bact** (character)

## New Columns in Modern System

- **registered_at** (timestamp without time zone)
- **first_name** (character varying)
- **claimant_id** (integer)
- **date_of_birth** (date)
- **phone_number** (bigint)
- **state** (character varying)
- **email** (character varying)
- **address_line1** (character varying)
- **ssn_hash** (character varying)
- **city** (character varying)
- **claimant_status** (character varying)
- **zip_code** (character varying)
- **is_deceased** (boolean)
- **last_name** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
