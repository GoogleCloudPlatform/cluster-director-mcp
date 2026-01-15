#!/usr/bin/env python3

import json
import os
import sys
import shutil
from pathlib import Path

# Parses a gemini extensions/settings file and adds 
# cluster-director-mcp and context7 if they are not already present

def add_extensions_to_gemini_json(file_path):
    """
    Adds cluster-director-mcp and context7 extensions servers to gemini JSON file.
    """

    print("Processing JSON settings file: " + file_path)

    shutil.copy2(json_file, json_file + ".orig")
    
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Parse error with JSON in {file_path}")
        print(f"Error Message: {e.msg}")
        print(f"Line Number:   {e.lineno}")
        print(f"Column Number: {e.colno}")
        print(f"Char Index:    {e.pos}")        
        return

    # Compute path to cluster-director-mcp
    current_dir = str(Path.cwd())
    mcp_binary_path = current_dir + "/cluster-director-mcp"
    gemini_md_path = current_dir + "/assets/GEMINI.md"

    if 'contextFileName' not in data:
        data['contextFileName'] = gemini_md_path
    elif data['contextFileName'] != gemini_md_path:
        data['context'] = {"fileName": gemini_md_path}

    if 'mcpServers' in data:
        mcp_servers_dict = data['mcpServers']

        # Cluster director MCP server needs to be added/updated to ensure the binary
        # path is correct
        print("Adding cluster-director-mcp MCP server")
        mcp_servers_dict['cluster-director-mcp'] = {
                "command": mcp_binary_path,
                "trust": True,
                "timeout": 72000000,
                "env": {
                    "MCP_SERVER_REQUEST_TIMEOUT": "72000000"
                }
            }
        if 'context7' not in mcp_servers_dict:
            print("Adding context7 MCP server")
            mcp_servers_dict['context7'] = {'httpUrl': "https://mcp.context7.com/mcp"}
        else:
            print("context7 MCP server already present")
    else:
        print("mcpServers not present in JSON file, adding both cluster-director-mcp and context7")
        data['mcpServers'] = {
            "cluster-director-mcp": {
                "command": mcp_binary_path,
                "trust": "true",
                "timeout": "72000000",
                "env": {
                    "MCP_SERVER_REQUEST_TIMEOUT": "72000000"
                }
            },
            'context7': {
                'httpUrl': "https://mcp.context7.com/mcp"
            }
        }

    # Write updated JSON
    with open(file_path, 'w') as file:
        # indent=4 makes the file human-readable (pretty-printed)
        # ensure_ascii=False ensures characters like emojis or accents aren't escaped
        json.dump(data, file, indent=4, ensure_ascii=False)
    

# --- Execution ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: installExtensions.py <JSON file with path>")
        exit(1)

    # Define our file and the new data
    json_file = sys.argv[1]

    folder_path = Path(json_file).parent

    # Create the directory
    # parents=True  -> Creates missing parent folders (like 'mkdir -p')
    # exist_ok=True -> Does nothing if the folder already exists (prevents errors)
    folder_path.mkdir(parents=True, exist_ok=True)

    # Create JSON
    if not os.path.exists(json_file):
        with open(json_file, 'w') as f:
            json.dump({"description": "AI assistant for Cluster Director to deploy and use GPU clusters."}, f)

    # Update JSON to have 
    add_extensions_to_gemini_json(json_file)
    
