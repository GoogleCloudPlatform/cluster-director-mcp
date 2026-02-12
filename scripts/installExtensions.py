#!/usr/bin/env python3

import json
import os
import sys
import shutil
import subprocess
from pathlib import Path

def add_extensions_to_gemini_json(file_path):
    """
    Adds Cluster Director AI Assistants (GKE and Slurm) and Google Compute MCP servers.
    Ensures 'tools' permissions are correctly set.
    """
    print(f"Processing JSON settings file: {file_path}")

    # Backup existing file
    if os.path.exists(file_path):
        try:
            shutil.copy2(file_path, file_path + ".orig")
        except OSError as e:
            print(f"Warning: Failed to create backup: {e}")

    #  Load existing JSON or create empty dict
    data = {}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in {file_path}: {e}")
            return
        except Exception as e:
            print(f"Error reading file: {e}")
            return

    # Define Paths (Using os.path.join for safety)
    current_dir = os.getcwd()
    slurm_mcp_binary = os.path.join(current_dir, "cluster-director-slurm", "cluster-director-slurm")
    gke_ai_mcp_binary = os.path.join(current_dir, "cluster-director-gke-ai", "cluster-director-gke-ai")
    gemini_md_path = os.path.join(current_dir, "assets", "GEMINI.md")

    # Set Context File
    data['contextFileName'] = gemini_md_path

    # Helper for Local Binary Configs
    def get_server_config(binary_path):
        return {
            "command": binary_path,
            "trust": True,
            "timeout": 72000000,
            "env": {
                "MCP_SERVER_REQUEST_TIMEOUT": "72000000",
                "GOOGLE_CLOUD_PROJECT": os.getenv("GOOGLE_CLOUD_PROJECT", "")
            }
        }

    # Configure MCP Servers
    if 'mcpServers' not in data:
        data['mcpServers'] = {}
    
    servers = data['mcpServers']

    # Remove legacy if present
    servers.pop('cluster-director-mcp', None)

    print("Adding cluster-director-gke-ai MCP server")
    servers['cluster-director-gke-ai'] = get_server_config(gke_ai_mcp_binary)

    print("Adding cluster-director-slurm MCP server")
    servers['cluster-director-slurm'] = get_server_config(slurm_mcp_binary)

    print("Adding google-compute-mcp MCP server (Implicit Auth)")
    servers['google-compute-mcp'] = {
        "httpUrl": "https://compute.googleapis.com/mcp",
        "authProviderType": "google_credentials",
        "oauth": {
            "scopes": ["https://www.googleapis.com/auth/compute.readonly"]
        },
        "trust": True,
        "timeout": 60000
    }

    # Configure Tools 
    # This explicitly enables the shell and file tools. 
    # Without this, the model hallucinates or fails.
    data['tools'] = {
        "core": [
            "run_shell_command",
            "read_file",
            "search_file_content",
            "save_memory"
        ]
    }

    #  Write Update
    try:
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
        print(f"Successfully updated {file_path}")
    except OSError as e:
        print(f"Error writing settings file: {e}")

# --- Execution ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: installExtensions.py <JSON file path>")
        sys.exit(1)

    json_file = sys.argv[1]
    folder_path = Path(json_file).parent
    
    try:
        folder_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"Error creating directory {folder_path}: {e}")
        sys.exit(1)

    # Initialize file if missing
    if not os.path.exists(json_file):
        with open(json_file, 'w') as f:
            json.dump({"description": "AI assistant for Cluster Director."}, f)

    add_extensions_to_gemini_json(json_file)
