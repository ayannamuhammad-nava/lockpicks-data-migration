# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_city** (character)
- **cl_adr1** (character varying)
- **cl_recid** (integer)
- **cl_stat** (character)
- **cl_rgdt** (character)
- **cl_dcsd** (character)
- **cl_emal** (character varying)
- **cl_fil1** (character)
- **cl_zip** (character)
- **cl_ssn** (character)
- **cl_st** (character)
- **cl_phon** (character)
- **cl_fnam** (character)
- **cl_dob** (character)
- **cl_bact** (character)
- **cl_brtn** (character)
- **cl_lnam** (character)

## New Columns in Modern System

- **phone_number** (bigint)
- **address_line1** (character varying)
- **registered_at** (timestamp without time zone)
- **first_name** (character varying)
- **state** (character varying)
- **date_of_birth** (date)
- **ssn_hash** (character varying)
- **is_deceased** (boolean)
- **city** (character varying)
- **last_name** (character varying)
- **claimant_status** (character varying)
- **claimant_id** (integer)
- **zip_code** (character varying)
- **email** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
