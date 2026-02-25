# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_fnam** (character)
- **cl_dcsd** (character)
- **cl_dob** (character)
- **cl_phon** (character)
- **cl_brtn** (character)
- **cl_recid** (integer)
- **cl_emal** (character varying)
- **cl_zip** (character)
- **cl_stat** (character)
- **cl_ssn** (character)
- **cl_adr1** (character varying)
- **cl_bact** (character)
- **cl_fil1** (character)
- **cl_lnam** (character)
- **cl_city** (character)
- **cl_st** (character)
- **cl_rgdt** (character)

## New Columns in Modern System

- **phone_number** (bigint)
- **claimant_status** (character varying)
- **is_deceased** (boolean)
- **zip_code** (character varying)
- **ssn_hash** (character varying)
- **registered_at** (timestamp without time zone)
- **state** (character varying)
- **last_name** (character varying)
- **claimant_id** (integer)
- **email** (character varying)
- **date_of_birth** (date)
- **first_name** (character varying)
- **address_line1** (character varying)
- **city** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
