#!/usr/bin/env python3
"""
Auto-generate Pandera schemas from database SQL schemas.

Usage:
    # Generate schemas for all tables in both databases
    python generate_schemas.py --all

    # Generate for specific tables
    python generate_schemas.py --tables claimants employers

    # Generate only for legacy system
    python generate_schemas.py --system legacy --tables claimants

    # Preview without saving
    python generate_schemas.py --tables claimants --preview
"""

import argparse
import sys
import yaml
from tools.db_utils import get_connection, process_config_env_vars
from tools.schema_loader import generate_pandera_schema, generate_schemas_for_database
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Auto-generate Pandera schemas from database tables"
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Config file path (default: config.yaml)'
    )
    parser.add_argument(
        '--system',
        choices=['legacy', 'modern', 'both'],
        default='both',
        help='Which database system to generate schemas for (default: both)'
    )
    parser.add_argument(
        '--tables',
        nargs='+',
        help='Specific tables to generate schemas for (e.g., claimants employers)'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Auto-detect and generate all tables in database'
    )
    parser.add_argument(
        '--preview',
        action='store_true',
        help='Preview generated schemas without saving to files'
    )
    parser.add_argument(
        '--output-dir',
        default='schemas',
        help='Output directory for generated schemas (default: schemas)'
    )

    args = parser.parse_args()

    # Load config
    try:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        config = process_config_env_vars(config)  # Process environment variables
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Determine which tables to generate
    if args.all:
        # Auto-detect tables from database
        logger.info("Auto-detecting tables from database...")
        tables = auto_detect_tables(config, args.system)
    elif args.tables:
        tables = args.tables
    else:
        logger.error("Either --tables or --all must be specified")
        parser.print_help()
        sys.exit(1)

    logger.info(f"Tables to process: {', '.join(tables)}")

    # Generate schemas
    systems = ['legacy', 'modern'] if args.system == 'both' else [args.system]

    for system in systems:
        logger.info(f"\n{'='*60}")
        logger.info(f"Generating schemas for {system.upper()} system")
        logger.info(f"{'='*60}\n")

        try:
            conn = get_connection(config['database'][system])

            if args.preview:
                # Preview mode - just print the code
                for table in tables:
                    try:
                        code = generate_pandera_schema(conn, table, system)
                        logger.info(f"\n--- {system}/{table}.py ---\n")
                        print(code)
                    except Exception as e:
                        logger.error(f"Failed to generate {table}: {e}")
            else:
                # Save to files
                results = generate_schemas_for_database(
                    conn,
                    system,
                    tables,
                    output_dir=args.output_dir
                )

                # Summary
                success = sum(1 for v in results.values() if v is not None)
                logger.info(f"\n✅ Generated {success}/{len(results)} schemas for {system}")

            conn.close()

        except Exception as e:
            logger.error(f"Error generating schemas for {system}: {e}")
            sys.exit(1)

    logger.info("\n" + "="*60)
    logger.info("Schema generation complete!")
    logger.info("="*60)


def auto_detect_tables(config: dict, system: str) -> list:
    """
    Auto-detect all tables in the database.

    Args:
        config: Configuration dict
        system: 'legacy', 'modern', or 'both'

    Returns:
        List of table names
    """
    import psycopg2

    systems = ['legacy', 'modern'] if system == 'both' else [system]

    # Use first system to detect tables
    first_system = systems[0]
    conn = get_connection(config['database'][first_system])

    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """

    with conn.cursor() as cur:
        cur.execute(query)
        tables = [row[0] for row in cur.fetchall()]

    conn.close()

    logger.info(f"Detected {len(tables)} tables: {', '.join(tables)}")
    return tables


if __name__ == '__main__':
    main()
