# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_dcsd** (character)
- **cl_adr1** (character varying)
- **cl_bact** (character)
- **cl_fil1** (character)
- **cl_lnam** (character)
- **cl_stat** (character)
- **cl_city** (character)
- **cl_fnam** (character)
- **cl_phon** (character)
- **cl_brtn** (character)
- **cl_emal** (character varying)
- **cl_zip** (character)
- **cl_recid** (integer)
- **cl_ssn** (character)
- **cl_rgdt** (character)
- **cl_st** (character)
- **cl_dob** (character)

## New Columns in Modern System

- **registered_at** (timestamp without time zone)
- **ssn_hash** (character varying)
- **city** (character varying)
- **last_name** (character varying)
- **address_line1** (character varying)
- **date_of_birth** (date)
- **is_deceased** (boolean)
- **zip_code** (character varying)
- **claimant_status** (character varying)
- **claimant_id** (integer)
- **phone_number** (bigint)
- **email** (character varying)
- **state** (character varying)
- **first_name** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
