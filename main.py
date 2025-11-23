"""
Main Entry Point
Run the complete automated 3NF data modeling workflow

Usage:
    python main.py
"""

import sys
from pathlib import Path
from langgraph_app import NormalizationWorkflow


def main():
    """Main execution function"""
    print("""
    ╔═══════════════════════════════════════════════════════════════════╗
    ║                                                                   ║
    ║         AUTOMATED 3NF DATA MODELING SYSTEM                       ║
    ║         Using Python + LangGraph                                 ║
    ║                                                                   ║
    ║  Features:                                                        ║
    ║  • Load unlimited CSV/JSON files                                 ║
    ║  • Extract comprehensive metadata                                ║
    ║  • Auto-detect Primary Keys & Foreign Keys                       ║
    ║  • Enforce 1NF, 2NF, 3NF normalization                          ║
    ║  • Generate Oracle SQL DDL scripts                               ║
    ║  • Export normalized tables (CSV/JSON)                           ║
    ║  • Generate ERD diagrams                                         ║
    ║                                                                   ║
    ╚═══════════════════════════════════════════════════════════════════╝
    """)
    
    # Configuration
    INPUT_FOLDER = "./input_files"
    OUTPUT_FOLDER = "./normalized_output"
    SQL_OUTPUT_FOLDER = "./sql_output"
    ERD_OUTPUT_FOLDER = "./erd"
    
    # Check if input folder exists
    input_path = Path(INPUT_FOLDER)
    if not input_path.exists():
        print(f"❌ Error: Input folder '{INPUT_FOLDER}' does not exist!")
        print(f"   Please create it and add your CSV/JSON files.")
        sys.exit(1)
    
    # Check if there are files to process
    csv_files = list(input_path.glob('*.csv'))
    json_files = list(input_path.glob('*.json'))
    total_files = len(csv_files) + len(json_files)
    
    if total_files == 0:
        print(f"❌ Error: No CSV or JSON files found in '{INPUT_FOLDER}'!")
        print(f"   Please add data files to process.")
        sys.exit(1)
    
    print(f"\n📁 Found {total_files} files to process:")
    print(f"   • CSV files: {len(csv_files)}")
    print(f"   • JSON files: {len(json_files)}")
    print(f"\n🚀 Starting normalization workflow...\n")
    
    try:
        # Create and run workflow
        workflow = NormalizationWorkflow()
        
        result = workflow.run(
            input_folder=INPUT_FOLDER,
            output_folder=OUTPUT_FOLDER,
            sql_output_folder=SQL_OUTPUT_FOLDER,
            erd_output_folder=ERD_OUTPUT_FOLDER
        )
        
        # Check final status
        if result['status'] == 'completed':
            print("\n" + "="*70)
            print("✅ WORKFLOW COMPLETED SUCCESSFULLY!")
            print("="*70)
            print("\n📊 Generated Outputs:")
            print(f"   1. Normalized Tables (CSV/JSON): {OUTPUT_FOLDER}/")
            if result.get('sql_script_path'):
                print(f"   2. SQL DDL Script: {result['sql_script_path']}")
            if result.get('erd_path'):
                print(f"   3. ERD Diagram: {result['erd_path']}")
            
            print("\n💡 Next Steps:")
            print("   • Review the normalized tables in the output folder")
            print("   • Import the SQL script into Oracle SQL Developer")
            print("   • Verify the ERD diagram for correctness")
            print("\n" + "="*70)
            
            return 0
        else:
            print("\n" + "="*70)
            print("⚠️  WORKFLOW COMPLETED WITH ISSUES")
            print("="*70)
            print(f"Status: {result['status']}")
            
            if result['errors']:
                print("\n❌ Errors encountered:")
                for error in result['errors']:
                    print(f"   • {error}")
            
            return 1
            
    except Exception as e:
        print("\n" + "="*70)
        print("❌ WORKFLOW FAILED")
        print("="*70)
        print(f"Error: {str(e)}")
        print("\n💡 Troubleshooting:")
        print("   1. Check that all required dependencies are installed")
        print("   2. Verify input files are valid CSV/JSON format")
        print("   3. Ensure sufficient disk space for outputs")
        
        import traceback
        print("\nFull error trace:")
        traceback.print_exc()
        
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
