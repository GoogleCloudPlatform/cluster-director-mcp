#!/usr/bin/env python3

import json
import os
import sys
import shutil
from pathlib import Path

# Parses a gemini extensions/settings file and adds 
# If Cluster Director AI Assistants are not already present

def add_extensions_to_gemini_json(file_path):
    """
    Adds cluster Director AI Assistants (GKE and Slurm) extensions servers to gemini JSON file.
    """

    print("Processing JSON settings file: " + file_path)

    # Note: json_file is defined in the global scope below
    shutil.copy2(file_path, file_path + ".orig")
    
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Parse error with JSON in {file_path}")
        print(f"Error Message: {e.msg}")
        return

    # Compute path to MCP servers
    current_dir = str(Path.cwd())
    slurm_mcp_binary_path = current_dir + "/cluster-director-slurm/cluster-director-slurm"
    gke_ai_mcp_binary_path = current_dir + "/cluster-director-gke-ai/cluster-director-gke-ai"
    gemini_md_path = current_dir + "/assets/GEMINI.md"

    if 'contextFileName' not in data:
        data['contextFileName'] = gemini_md_path
    elif data['contextFileName'] != gemini_md_path:
        data['context'] = {"fileName": gemini_md_path}

    # Standard configuration for the local Go binaries
    def get_server_config(binary_path):
        return {
            "command": binary_path,
            "trust": True,
            "timeout": 72000000,
            "env": {
                "MCP_SERVER_REQUEST_TIMEOUT": "72000000"
            }
        }

    if 'mcpServers' not in data:
        data['mcpServers'] = {}

    mcp_servers_dict = data['mcpServers']

    # Remove old or unwanted servers
    unwanted = ['cluster-director-mcp', 'VertexMcpServer', 'vertex']
    for server in unwanted:
        if server in mcp_servers_dict:
            print(f"Removing unwanted server: {server}")
            del mcp_servers_dict[server]

    # ADD/UPDATE LOCAL SERVERS
    print("Adding cluster-director-gke-ai MCP server")
    mcp_servers_dict['cluster-director-gke-ai'] = get_server_config(gke_ai_mcp_binary_path)

    print("Adding cluster-director-slurm MCP server")
    mcp_servers_dict['cluster-director-slurm'] = get_server_config(slurm_mcp_binary_path)

    # ADD/UPDATE REMOTE GOOGLE COMPUTE SERVER
    print("Adding google-compute-mcp server")
    mcp_servers_dict['google-compute-mcp'] = {
        "url": "https://compute.googleapis.com/mcp",
        "trust": True
    }

    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: installExtensions.py <JSON file with path>")
        exit(1)

    json_file = sys.argv[1]
    folder_path = Path(json_file).parent
    folder_path.mkdir(parents=True, exist_ok=True)

    if not os.path.exists(json_file):
        with open(json_file, 'w') as f:
            json.dump({"description": "AI assistant for Cluster Director."}, f)

    add_extensions_to_gemini_json(json_file)
