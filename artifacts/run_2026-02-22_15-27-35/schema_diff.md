# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_rgdt** (character)
- **cl_bact** (character)
- **cl_dcsd** (character)
- **cl_ssn** (character)
- **cl_lnam** (character)
- **cl_brtn** (character)
- **cl_city** (character)
- **cl_dob** (character)
- **cl_st** (character)
- **cl_emal** (character varying)
- **cl_fil1** (character)
- **cl_recid** (integer)
- **cl_fnam** (character)
- **cl_stat** (character)
- **cl_zip** (character)
- **cl_phon** (character)
- **cl_adr1** (character varying)

## New Columns in Modern System

- **phone_number** (bigint)
- **state** (character varying)
- **claimant_id** (integer)
- **zip_code** (character varying)
- **email** (character varying)
- **claimant_status** (character varying)
- **registered_at** (timestamp without time zone)
- **ssn_hash** (character varying)
- **first_name** (character varying)
- **is_deceased** (boolean)
- **date_of_birth** (date)
- **address_line1** (character varying)
- **last_name** (character varying)
- **city** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
