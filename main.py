#!/usr/bin/env python3
"""
Data Validation Agent - CLI Entry Point
Main command-line interface for running data validation across migration phases.
"""
import argparse
import sys
import yaml
from agents.orchestrator import run_agent
from tools import db_utils, metadata_generator
from tools.db_utils import process_config_env_vars


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Data Validation Agent - Validate legacy-to-modern data migrations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Pre-migration validation
  python main.py --phase pre --dataset claimants --sample 500

  # Post-migration reconciliation
  python main.py --phase post --dataset claimants

Phases:
  pre     - Detect structural & governance risks before migration
  post    - Prove integrity & reconciliation after migration
        """
    )

    parser.add_argument(
        '--phase',
        choices=['pre', 'post'],
        help='Validation phase to run'
    )

    parser.add_argument(
        '--dataset',
        default='claimants',
        help='Dataset to validate (default: claimants)'
    )

    parser.add_argument(
        '--sample',
        type=int,
        default=1000,
        help='Sample size for validation (default: 1000)'
    )

    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )

    parser.add_argument(
        '--generate-metadata',
        action='store_true',
        help='Auto-generate RAG metadata (glossary.json and mappings.json) from database schemas'
    )

    parser.add_argument(
        '--no-interactive',
        action='store_true',
        help='Disable interactive prompts (auto-accept all generated metadata)'
    )

    parser.add_argument(
        '--tables',
        nargs='+',
        default=None,
        help='Tables to generate metadata for (default: read from config.yaml validation.tables)'
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.generate_metadata and not args.phase:
        parser.error("Either --phase or --generate-metadata is required")

    try:
        # Handle metadata generation
        if args.generate_metadata:
            # Load config
            with open(args.config, 'r') as f:
                config = yaml.safe_load(f)
            config = process_config_env_vars(config)  # Process environment variables

            # Resolve tables from config if not specified on CLI
            tables = args.tables or config.get('validation', {}).get('tables', ['claimants'])

            # Connect to databases
            legacy_conn = db_utils.get_connection(config['database']['legacy'])
            modern_conn = db_utils.get_connection(config['database']['modern'])

            # Generate metadata
            metadata_generator.generate_metadata(
                legacy_conn,
                modern_conn,
                tables=tables,
                interactive=not args.no_interactive,
                confidence_threshold=0.7,
                output_dir='./metadata'
            )

            legacy_conn.close()
            modern_conn.close()

            sys.exit(0)

        # Run validation
        result = run_agent(
            phase=args.phase,
            dataset=args.dataset,
            sample_size=args.sample,
            config_path=args.config
        )

        # Print enhanced summary with confidence breakdown
        print("\n" + "=" * 60)
        print("           VALIDATION COMPLETE")
        print("=" * 60)
        print(f"\nPhase:       {args.phase.upper()}")
        print(f"Dataset:     {args.dataset}")
        print("")
        print("=" * 60)
        print(f"  MIGRATION CONFIDENCE: {result['score']}/100")

        # Status emoji
        status_emoji = "🟢" if result['status'] == 'GREEN' else "🟡" if result['status'] == 'YELLOW' else "🔴"
        print(f"  STATUS: {status_emoji} {result['status']}")
        print("=" * 60)
        print("")
        print(f"Artifacts:   {result['artifact_path']}")

        # Show dashboard file if it exists (for pre-phase)
        import os
        dashboard_path = os.path.join(result['artifact_path'], 'READINESS_DASHBOARD.md')
        if os.path.exists(dashboard_path):
            print(f"Dashboard:   {dashboard_path}")

        print("=" * 60)

        # Exit with appropriate code and clear messaging
        if result['status'] == 'RED':
            print("\n🔴 VALIDATION FAILED")
            print("   Action Required: Review artifacts and fix issues before proceeding")
            print("")
            sys.exit(1)
        elif result['status'] == 'YELLOW':
            print("\n🟡 VALIDATION WARNING")
            print("   Recommendation: Review findings and address warnings")
            print("")
            sys.exit(0)
        else:
            print("\n🟢 VALIDATION PASSED")
            print("   Status: Safe to proceed with migration")
            print("")
            sys.exit(0)

    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == '__main__':
    main()
