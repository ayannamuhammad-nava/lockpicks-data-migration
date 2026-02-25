# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_bact** (character)
- **cl_rgdt** (character)
- **cl_adr1** (character varying)
- **cl_lnam** (character)
- **cl_phon** (character)
- **cl_city** (character)
- **cl_zip** (character)
- **cl_dcsd** (character)
- **cl_ssn** (character)
- **cl_recid** (integer)
- **cl_fnam** (character)
- **cl_emal** (character varying)
- **cl_st** (character)
- **cl_dob** (character)
- **cl_brtn** (character)
- **cl_fil1** (character)
- **cl_stat** (character)

## New Columns in Modern System

- **first_name** (character varying)
- **state** (character varying)
- **claimant_id** (integer)
- **last_name** (character varying)
- **claimant_status** (character varying)
- **date_of_birth** (date)
- **city** (character varying)
- **phone_number** (bigint)
- **email** (character varying)
- **zip_code** (character varying)
- **ssn_hash** (character varying)
- **registered_at** (timestamp without time zone)
- **address_line1** (character varying)
- **is_deceased** (boolean)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
