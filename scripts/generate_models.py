#!/usr/bin/env python3
"""Generate Pydantic models from OpenAPI spec.

This script uses datamodel-code-generator to generate Pydantic v2 models
from the Admin API OpenAPI specification.

Usage:
    uv run python scripts/generate_models.py
    # or via Makefile:
    make generate-models
"""

from pathlib import Path


def main():
    """Generate Pydantic models from OpenAPI spec."""
    from datamodel_code_generator import DataModelType, InputFileType, generate

    # Define paths
    spec_file = Path('specs/admin.spec.json')
    output_file = Path('src/amp/admin/models.py')

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Validate spec file exists
    if not spec_file.exists():
        raise FileNotFoundError(f'OpenAPI spec not found: {spec_file}\nPlease ensure the spec file is in place.')

    print(f'Generating Pydantic models from {spec_file}...')

    # Generate models
    generate(
        input_=spec_file,
        output=output_file,
        input_file_type=InputFileType.OpenAPI,
        output_model_type=DataModelType.PydanticV2BaseModel,
        use_schema_description=True,
        use_field_description=True,
        field_constraints=True,
        use_standard_collections=True,
        use_annotated=True,
    )

    print(f'✅ Successfully generated models: {output_file}')
    print(f'   Lines generated: {len(output_file.read_text().splitlines())}')


if __name__ == '__main__':
    main()
