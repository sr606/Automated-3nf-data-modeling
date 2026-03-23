#!/usr/bin/env python3
"""
Unified Lineage Agent Entry Point
Processes all files in data/uploads folder and generates lineage outputs
"""

import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from lineage_agent.brain import ensure_data_layout, run_lineage_pipeline


load_dotenv()

DEFAULT_INPUT_DIR = Path("data") / "uploads"
DEFAULT_OUTPUT_DIR = Path("data") / "output"


def find_input_files(uploads_dir: Path) -> list[Path]:
    """Find all input files in the uploads directory."""
    if not uploads_dir.exists():
        return []

    # Look for text files (pseudocode, XML exports, etc.)
    files = list(uploads_dir.glob("*.txt"))
    files.extend(uploads_dir.glob("*.xml"))
    files.extend(uploads_dir.glob("*.json"))

    return sorted(files)


def process_single_file(input_file: Path, output_dir: Path, model_override: str = "") -> dict[str, Any]:
    """Process a single input file through the lineage pipeline."""
    print(f"\n{'=' * 80}")
    print(f"Processing: {input_file.name}")
    print(f"{'=' * 80}")

    try:
        result = run_lineage_pipeline(
            input_path=str(input_file),
            output_dir=str(output_dir),
            model_override=model_override,
        )
        result["status"] = "success"
        result["input_file"] = str(input_file)
        print(f"Successfully processed {input_file.name}")
        return result
    except Exception as e:
        error_result = {
            "status": "error",
            "input_file": str(input_file),
            "error": str(e),
        }
        print(f"Error processing {input_file.name}: {e}")
        return error_result


def main() -> None:
    """Main entry point: process all files in uploads folder."""
    print("\n" + "=" * 80)
    print("LINEAGE AGENT - UNIFIED PIPELINE")
    print("=" * 80)

    # Ensure directory structure exists
    ensure_data_layout()
    DEFAULT_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Find all input files
    input_files = find_input_files(DEFAULT_INPUT_DIR)

    if not input_files:
        print(f"\nNo input files found in {DEFAULT_INPUT_DIR}")
        print(f"Please add pseudocode or XML files to: {DEFAULT_INPUT_DIR.resolve()}")
        return

    print(f"\nFound {len(input_files)} input file(s) in {DEFAULT_INPUT_DIR}")
    for file in input_files:
        print(f"   - {file.name}")

    # Process each file
    results = []
    for input_file in input_files:
        result = process_single_file(input_file, DEFAULT_OUTPUT_DIR)
        results.append(result)

    # Summary
    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)

    successful = sum(1 for r in results if r.get("status") == "success")
    failed = len(results) - successful

    print(f"\nSuccessful: {successful}/{len(results)}")
    print(f"Failed: {failed}/{len(results)}")

    if successful > 0:
        print(f"\nOutput files generated in: {DEFAULT_OUTPUT_DIR.resolve()}")
        print("\nGenerated outputs:")
        for i, result in enumerate(results, 1):
            if result.get("status") == "success":
                input_name = Path(result.get("input_file", "")).name
                run_dir = result.get("run_dir", "")
                if run_dir:
                    print(f"   {i}. {input_name}")
                    print(f"      {run_dir}")
                    run_path = Path(run_dir)
                    if run_path.exists():
                        dot_files = list(run_path.glob("*.dot"))
                        pdf_files = list(run_path.glob("*.pdf"))
                        if dot_files:
                            print(f"      Diagrams (DOT): {len(dot_files)} file(s)")
                        if pdf_files:
                            print(f"      Diagrams (PDF): {len(pdf_files)} file(s)")
                        elif result.get("warnings"):
                            print("      PDF generation warning:")
                            print(f"      {result['warnings'][0]}")

    if failed > 0:
        print("\nErrors:")
        for i, result in enumerate(results, 1):
            if result.get("status") == "error":
                input_name = Path(result.get("input_file", "")).name
                error = result.get("error", "Unknown error")
                print(f"   {i}. {input_name}: {error}")

    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)
