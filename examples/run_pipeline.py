#!/usr/bin/env python3
"""
Hindi PDF Processing Pipeline - Example Usage Script

This script demonstrates how to use the Hindi PDF processing pipeline
for automated PDF processing from Google Drive.

Usage:
    python run_pipeline.py [--mode MODE] [--config CONFIG_FILE]

Modes:
    - single: Run a single processing cycle
    - continuous: Run continuously with polling
    - scheduled: Run on a schedule
    - status: Show pipeline status
    - reprocess: Reprocess failed files
"""

import sys
import argparse
import logging
import schedule
import time
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hindi_pdf_pipeline.main_pipeline import HindiPDFPipeline
from hindi_pdf_pipeline.config import get_config, reload_config

def setup_logging():
    """Set up basic logging for the example script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('examples/pipeline_example.log')
        ]
    )

def run_single_cycle():
    """Run a single processing cycle."""
    print("Running single processing cycle...")
    
    try:
        # Initialize pipeline
        pipeline = HindiPDFPipeline()
        
        # Run single cycle
        results = pipeline.run_single_cycle()
        
        # Print results
        if results['status'] == 'success':
            print(f"✅ Processing completed successfully!")
            print(f"   Processed: {results.get('processed', 0)} files")
            print(f"   Failed: {results.get('failed', 0)} files") 
            print(f"   Skipped: {results.get('skipped', 0)} files")
            print(f"   Time: {results.get('cycle_time_seconds', 0):.2f} seconds")
        else:
            print(f"❌ Processing failed: {results.get('message', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error running pipeline: {e}")
        logging.error(f"Pipeline error: {e}", exc_info=True)

def run_continuous_processing():
    """Run continuous processing with polling."""
    print("Starting continuous processing...")
    print("Press Ctrl+C to stop")
    
    try:
        # Initialize pipeline
        pipeline = HindiPDFPipeline()
        
        # Run continuously (this will block)
        pipeline.run_continuous()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping continuous processing...")
    except Exception as e:
        print(f"❌ Error in continuous processing: {e}")
        logging.error(f"Continuous processing error: {e}", exc_info=True)

def run_scheduled_processing():
    """Run scheduled processing."""
    print("Setting up scheduled processing...")
    
    try:
        # Initialize pipeline
        pipeline = HindiPDFPipeline()
        
        # Schedule processing every hour
        schedule.every().hour.do(pipeline.run_scheduled)
        
        # Schedule daily maintenance at 2 AM
        # schedule.every().day.at("02:00").do(pipeline._run_maintenance_tasks)
        
        print("⏰ Scheduled processing configured:")
        print("   - Processing: Every hour")
        print("   - Maintenance: Daily at 2:00 AM")
        print("\nPress Ctrl+C to stop")
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping scheduled processing...")
    except Exception as e:
        print(f"❌ Error in scheduled processing: {e}")
        logging.error(f"Scheduled processing error: {e}", exc_info=True)

def show_status():
    """Show pipeline status and statistics."""
    print("Fetching pipeline status...")
    
    try:
        # Initialize pipeline
        pipeline = HindiPDFPipeline()
        
        # Get status
        status = pipeline.get_status()
        
        # Display pipeline status
        print("\n📊 Pipeline Status:")
        print(f"   Running: {'Yes' if status['pipeline']['is_running'] else 'No'}")
        print(f"   Uptime: {status['pipeline']['uptime_seconds']:.0f} seconds")
        print(f"   Total runs: {status['pipeline']['runs_count']}")
        print(f"   Files processed: {status['pipeline']['files_processed']}")
        print(f"   Files failed: {status['pipeline']['files_failed']}")
        
        if status['pipeline']['last_run']:
            print(f"   Last run: {status['pipeline']['last_run']}")
        
        # Display file tracker statistics
        print("\n📁 File Tracker Statistics:")
        tracker_stats = status['file_tracker']
        print(f"   Total files tracked: {tracker_stats['total_files']}")
        print(f"   Success rate: {tracker_stats['success_rate_percent']:.1f}%")
        
        if tracker_stats['status_counts']:
            print("   Status breakdown:")
            for status_name, count in tracker_stats['status_counts'].items():
                if count > 0:
                    print(f"     {status_name}: {count}")
        
        # Display recent errors
        if status['pipeline']['recent_errors']:
            print(f"\n⚠️  Recent errors ({len(status['pipeline']['recent_errors'])}):")
            for error in status['pipeline']['recent_errors'][-3:]:  # Show last 3
                print(f"   {error['timestamp']}: {error.get('filename', 'Unknown')} - {error['error']}")
        
        print("\n✅ Status check completed")
        
    except Exception as e:
        print(f"❌ Error getting status: {e}")
        logging.error(f"Status error: {e}", exc_info=True)

def reprocess_failed_files():
    """Reprocess files that previously failed."""
    print("Starting reprocessing of failed files...")
    
    try:
        # Initialize pipeline
        pipeline = HindiPDFPipeline()
        
        # Reprocess failed files
        results = pipeline.reprocess_failed_files()
        
        # Display results
        if results['status'] == 'success':
            print("✅ Reprocessing completed!")
            print(f"   Total files attempted: {results['total_files']}")
            print(f"   Successfully processed: {results['processed']}")
            print(f"   Failed again: {results['failed']}")
            print(f"   Skipped: {results['skipped']}")
        else:
            print(f"❌ Reprocessing failed: {results.get('message', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error reprocessing files: {e}")
        logging.error(f"Reprocessing error: {e}", exc_info=True)

def process_single_file(file_id: str):
    """Process a single file by Google Drive ID."""
    print(f"Processing single file: {file_id}")
    
    try:
        # Initialize pipeline
        pipeline = HindiPDFPipeline()
        
        # Process the file
        success = pipeline.process_file_by_id(file_id)
        
        if success:
            print(f"✅ File {file_id} processed successfully!")
        else:
            print(f"❌ Failed to process file {file_id}")
            
    except Exception as e:
        print(f"❌ Error processing file {file_id}: {e}")
        logging.error(f"Single file processing error: {e}", exc_info=True)

def generate_report(output_path: str):
    """Generate a comprehensive processing report."""
    print(f"Generating processing report: {output_path}")
    
    try:
        # Initialize pipeline
        pipeline = HindiPDFPipeline()
        
        # Generate report
        report_path = pipeline.export_processing_report(output_path)
        
        print(f"✅ Processing report generated: {report_path}")
        
    except Exception as e:
        print(f"❌ Error generating report: {e}")
        logging.error(f"Report generation error: {e}", exc_info=True)

def main():
    """Main function to handle command line arguments and run appropriate mode."""
    parser = argparse.ArgumentParser(
        description="Hindi PDF Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --mode single
  python run_pipeline.py --mode continuous
  python run_pipeline.py --mode status
  python run_pipeline.py --mode reprocess
  python run_pipeline.py --mode file --file-id 1abc123def456
  python run_pipeline.py --mode report --output reports/status.json
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['single', 'continuous', 'scheduled', 'status', 'reprocess', 'file', 'report'],
        default='single',
        help='Processing mode (default: single)'
    )
    
    parser.add_argument(
        '--config',
        help='Path to configuration file (.env)'
    )
    
    parser.add_argument(
        '--file-id',
        help='Google Drive file ID (for file mode)'
    )
    
    parser.add_argument(
        '--output',
        help='Output path for reports'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    setup_logging()
    
    # Load custom configuration if provided
    if args.config:
        print(f"Loading configuration from: {args.config}")
        reload_config(args.config)
    
    # Run appropriate mode
    try:
        if args.mode == 'single':
            run_single_cycle()
        elif args.mode == 'continuous':
            run_continuous_processing()
        elif args.mode == 'scheduled':
            run_scheduled_processing()
        elif args.mode == 'status':
            show_status()
        elif args.mode == 'reprocess':
            reprocess_failed_files()
        elif args.mode == 'file':
            if not args.file_id:
                print("❌ --file-id is required for file mode")
                sys.exit(1)
            process_single_file(args.file_id)
        elif args.mode == 'report':
            output_path = args.output or f"reports/processing_report_{int(time.time())}.json"
            generate_report(output_path)
        else:
            print(f"❌ Unknown mode: {args.mode}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Operation interrupted by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
