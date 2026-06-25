#!/bin/bash
# Pre-renderer demo script
# This script prints the received content and returns it unchanged

set -e

# Use python3 -c to execute inline code, reading from stdin
python3 -c "
import json
import sys
import base64

try:
    # Read JSON input from stdin
    input_data = json.load(sys.stdin)
    
    # Print the received content to stderr (so it doesn't interfere with JSON output)
    print('=== Pre-Renderer Demo: Received Content ===', file=sys.stderr)
    print('', file=sys.stderr)
    print('Input JSON:', file=sys.stderr)
    print(json.dumps(input_data, indent=2), file=sys.stderr)
    print('', file=sys.stderr)
    
    print('=== Parsed Components ===', file=sys.stderr)
    print('', file=sys.stderr)
    
    chart_templates = input_data.get('chartTemplates', [])
    values = input_data.get('values', {})
    chart_metadata = input_data.get('chartMetadata', {})
    extra_args = input_data.get('extraArgs', [])
    
    print(f'Chart Templates (count): {len(chart_templates)}', file=sys.stderr)
    print('', file=sys.stderr)
    
    print('Values:', file=sys.stderr)
    print(json.dumps(values, indent=2), file=sys.stderr)
    print('', file=sys.stderr)
    
    print('Chart Metadata:', file=sys.stderr)
    print(json.dumps(chart_metadata, indent=2), file=sys.stderr)
    print('', file=sys.stderr)
    
    if extra_args:
        print(f'Extra Args: {extra_args}', file=sys.stderr)
        print('', file=sys.stderr)
    
    # Print template details
    if chart_templates:
        print('=== Template Details ===', file=sys.stderr)
        print('Templates:', file=sys.stderr)
        for i, template in enumerate(chart_templates):
            template_name = template.get('name', 'unknown')
            data_size = len(template.get('data', ''))
            print(f'  [{i+1}] {template_name} (data size: {data_size} bytes)', file=sys.stderr)
        print('', file=sys.stderr)
        
        # Show sample template content
        if len(chart_templates) > 0:
            first_template = chart_templates[0]
            template_name = first_template.get('name', 'unknown')
            template_data_b64 = first_template.get('data', '')
            
            if template_data_b64:
                try:
                    template_data = base64.b64decode(template_data_b64).decode('utf-8')
                    print(f'=== Sample Template Content ({template_name}) ===', file=sys.stderr)
                    preview = template_data[:500]
                    if len(template_data) > 500:
                        preview += '\n... (truncated)'
                    print(preview, file=sys.stderr)
                    print('', file=sys.stderr)
                except Exception as e:
                    print(f'Error decoding template: {e}', file=sys.stderr)
    
    # Return the input unchanged
    output = {
        'chartTemplates': chart_templates,
        'values': values
    }
    
    print(json.dumps(output))
    
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
"
