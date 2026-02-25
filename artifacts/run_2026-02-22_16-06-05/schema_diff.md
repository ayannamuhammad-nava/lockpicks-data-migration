# Schema Diff Report: claimants

## Columns Missing in Modern System

- **cl_bact** (character)
- **cl_dcsd** (character)
- **cl_fil1** (character)
- **cl_phon** (character)
- **cl_adr1** (character varying)
- **cl_fnam** (character)
- **cl_zip** (character)
- **cl_city** (character)
- **cl_emal** (character varying)
- **cl_ssn** (character)
- **cl_lnam** (character)
- **cl_dob** (character)
- **cl_brtn** (character)
- **cl_rgdt** (character)
- **cl_recid** (integer)
- **cl_stat** (character)
- **cl_st** (character)

## New Columns in Modern System

- **first_name** (character varying)
- **state** (character varying)
- **address_line1** (character varying)
- **claimant_status** (character varying)
- **ssn_hash** (character varying)
- **claimant_id** (integer)
- **last_name** (character varying)
- **date_of_birth** (date)
- **phone_number** (bigint)
- **email** (character varying)
- **registered_at** (timestamp without time zone)
- **is_deceased** (boolean)
- **city** (character varying)
- **zip_code** (character varying)

## Summary

- Common columns: 0
- Missing in modern: 17
- New in modern: 14
- Type mismatches: 0
