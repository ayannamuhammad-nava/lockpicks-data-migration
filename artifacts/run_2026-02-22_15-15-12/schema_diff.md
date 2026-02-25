# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_ssn** (character)
- **cl_dob** (character)
- **cl_recid** (integer)
- **cl_adr1** (character varying)
- **cl_zip** (character)
- **cl_bact** (character)
- **cl_emal** (character varying)
- **cl_brtn** (character)
- **cl_city** (character)
- **cl_st** (character)
- **cl_lnam** (character)
- **cl_rgdt** (character)
- **cl_phon** (character)
- **cl_dcsd** (character)
- **cl_fil1** (character)
- **cl_stat** (character)
- **cl_fnam** (character)

## New Columns in Modern System

- **is_deceased** (boolean)
- **phone_number** (bigint)
- **state** (character varying)
- **claimant_id** (integer)
- **registered_at** (timestamp without time zone)
- **zip_code** (character varying)
- **first_name** (character varying)
- **date_of_birth** (date)
- **address_line1** (character varying)
- **claimant_status** (character varying)
- **ssn_hash** (character varying)
- **city** (character varying)
- **email** (character varying)
- **last_name** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
