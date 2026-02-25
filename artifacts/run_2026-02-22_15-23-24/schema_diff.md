# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_zip** (character)
- **cl_ssn** (character)
- **cl_bact** (character)
- **cl_lnam** (character)
- **cl_fnam** (character)
- **cl_dob** (character)
- **cl_phon** (character)
- **cl_st** (character)
- **cl_adr1** (character varying)
- **cl_fil1** (character)
- **cl_dcsd** (character)
- **cl_recid** (integer)
- **cl_emal** (character varying)
- **cl_city** (character)
- **cl_brtn** (character)
- **cl_rgdt** (character)
- **cl_stat** (character)

## New Columns in Modern System

- **zip_code** (character varying)
- **date_of_birth** (date)
- **state** (character varying)
- **registered_at** (timestamp without time zone)
- **last_name** (character varying)
- **city** (character varying)
- **email** (character varying)
- **first_name** (character varying)
- **address_line1** (character varying)
- **claimant_status** (character varying)
- **ssn_hash** (character varying)
- **claimant_id** (integer)
- **is_deceased** (boolean)
- **phone_number** (bigint)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
