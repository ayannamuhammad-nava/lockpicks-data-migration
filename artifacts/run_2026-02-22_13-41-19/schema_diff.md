# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_fnam** (character)
- **cl_brtn** (character)
- **cl_lnam** (character)
- **cl_emal** (character varying)
- **cl_adr1** (character varying)
- **cl_rgdt** (character)
- **cl_st** (character)
- **cl_dob** (character)
- **cl_phon** (character)
- **cl_stat** (character)
- **cl_fil1** (character)
- **cl_city** (character)
- **cl_recid** (integer)
- **cl_bact** (character)
- **cl_zip** (character)
- **cl_ssn** (character)
- **cl_dcsd** (character)

## New Columns in Modern System

- **email** (character varying)
- **state** (character varying)
- **phone_number** (bigint)
- **last_name** (character varying)
- **date_of_birth** (date)
- **address_line1** (character varying)
- **claimant_id** (integer)
- **is_deceased** (boolean)
- **first_name** (character varying)
- **registered_at** (timestamp without time zone)
- **ssn_hash** (character varying)
- **claimant_status** (character varying)
- **city** (character varying)
- **zip_code** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
