# Schema Diff Report: claimants

## Columns Missing in Modern System

- **clmt_email** (character varying)
- **clmt_id** (integer)
- **clmt_dob** (character varying)
- **clmt_phone** (character varying)
- **clmt_first_nm** (character varying)
- **bank_acct_num** (character varying)
- **clmt_addr** (character varying)
- **clmt_ssn** (character varying)
- **clmt_zip** (character varying)
- **clmt_city** (character varying)
- **bank_routing_num** (character varying)
- **clmt_state** (character varying)
- **clmt_last_nm** (character varying)
- **registration_dt** (timestamp without time zone)
- **clmt_status** (character varying)

## New Columns in Modern System

- **city** (character varying)
- **registered_at** (timestamp without time zone)
- **first_name** (character varying)
- **ssn_hash** (character varying)
- **claimant_status** (character varying)
- **claimant_id** (integer)
- **address_line1** (character varying)
- **zip_code** (character varying)
- **state** (character varying)
- **phone_number** (bigint)
- **date_of_birth** (date)
- **email** (character varying)
- **last_name** (character varying)

## Summary

- Common columns: 1
- Missing in modern: 15
- New in modern: 13
- Type mismatches: 0
