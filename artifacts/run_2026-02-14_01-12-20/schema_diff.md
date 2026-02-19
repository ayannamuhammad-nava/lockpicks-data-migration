# Schema Diff Report: customers

## Columns Missing in Modern System

- **ssn** (character varying)
- **created_date** (timestamp without time zone)
- **customer_name** (character varying)

## New Columns in Modern System

- **full_name** (character varying)
- **created_at** (timestamp without time zone)

## Type Mismatches

| Column | Legacy Type | Modern Type |
|--------|-------------|-------------|
| phone | character varying | bigint |

## Summary

- Common columns: 4
- Missing in modern: 3
- New in modern: 2
- Type mismatches: 1
