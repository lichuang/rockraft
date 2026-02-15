#!/usr/bin/env python3
"""
Test cluster membership functionality using pytest.

This test:
1. Builds the project
2. Starts the cluster using start.sh
3. Queries the /members endpoint on each node
4. Verifies that all nodes report consistent membership information
5. Stops the cluster
"""

import subprocess
import sys
import time
import json
import urllib.request
import urllib.error
import pytest
from pathlib import Path
from typing import Dict, Any, Optional

# Configuration
NODES = ["node1", "node2", "node3"]
HTTP_PORTS = [8001, 8002, 8003]
BASE_DIR = Path(__file__).parent.parent
START_SCRIPT = BASE_DIR / "start.sh"


class ClusterManager:
    """Manages the cluster lifecycle for testing."""
    
    def __init__(self, base_dir: Path, start_script: Path):
        self.base_dir = base_dir
        self.start_script = start_script
        self._is_running = False
    
    def run_command(self, cmd: list, check: bool = True) -> subprocess.CompletedProcess:
        """Run a shell command and return the result."""
        print(f"[CMD] {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            cwd=self.base_dir,
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            print(f"[ERROR] Command failed: {result.stderr}")
            raise RuntimeError(f"Command failed: {cmd}")
        return result
    
    def build(self) -> None:
        """Build the project."""
        print("\n[BUILD] Building the project...")
        self.run_command(["cargo", "build"])
        print("[BUILD] Build successful")
    
    def clean(self) -> None:
        """Clean up existing data."""
        print("\n[CLEAN] Cleaning up existing data...")
        self.run_command(["bash", str(self.start_script), "clean-all"], check=False)
        time.sleep(1)
    
    def start(self) -> None:
        """Start the cluster."""
        print("\n[START] Starting cluster...")
        result = self.run_command(["bash", str(self.start_script), "start", "warn"])
        self._is_running = True
        print(result.stdout)
    
    def stop(self) -> None:
        """Stop the cluster."""
        print("\n[STOP] Stopping cluster...")
        self.run_command(["bash", str(self.start_script), "stop"], check=False)
        self._is_running = False
        print("[STOP] Cluster stopped")
    
    def is_running(self) -> bool:
        """Check if cluster is running."""
        return self._is_running
    
    def __enter__(self):
        """Context manager entry."""
        self.clean()
        self.build()
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        return False


class ClusterClient:
    """Client for interacting with the cluster HTTP API."""
    
    def __init__(self, ports: list):
        self.ports = ports
    
    def query_members(self, port: int, timeout: int = 5) -> Dict[str, Any]:
        """Query the members endpoint of a node."""
        url = f"http://127.0.0.1:{port}/members"
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            raise RuntimeError(f"Failed to query members on port {port}: {e}")
    
    def wait_for_nodes(self, max_retries: int = 30, retry_interval: float = 1.0) -> None:
        """Wait for all nodes to be ready."""
        print("\n[WAIT] Waiting for nodes to be ready...")
        for port in self.ports:
            self._wait_for_node(port, max_retries, retry_interval)
        print("[WAIT] All nodes are ready")
    
    def _wait_for_node(self, port: int, max_retries: int, retry_interval: float) -> None:
        """Wait for a single node to be ready."""
        url = f"http://127.0.0.1:{port}/health"
        for i in range(max_retries):
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    if response.status == 200:
                        print(f"  Node on port {port} is ready")
                        return
            except Exception:
                pass
            time.sleep(retry_interval)
        raise RuntimeError(f"Node on port {port} failed to start after {max_retries} retries")
    
    def wait_for_members_sync(self, expected_count: int = 3, max_retries: int = 60) -> None:
        """Wait for all nodes to have synced members."""
        print("\n[SYNC] Waiting for cluster membership to sync...")
        for i, port in enumerate(self.ports):
            self._wait_for_members_sync(port, expected_count, max_retries)
            print(f"  Node {NODES[i]} (port {port}) has synced members")
        print("[SYNC] All nodes have synced members")
    
    def _wait_for_members_sync(self, port: int, expected_count: int, max_retries: int) -> None:
        """Wait for a single node to have synced members."""
        for i in range(max_retries):
            try:
                result = self.query_members(port)
                if result.get("success"):
                    members = result.get("members", {})
                    if len(members) >= expected_count:
                        return
            except Exception:
                pass
            time.sleep(0.5)
        raise RuntimeError(f"Node on port {port} failed to sync members after {max_retries} retries")


@pytest.fixture(scope="module")
def cluster():
    """Pytest fixture to manage cluster lifecycle."""
    cluster_mgr = ClusterManager(BASE_DIR, START_SCRIPT)
    with cluster_mgr:
        client = ClusterClient(HTTP_PORTS)
        client.wait_for_nodes()
        client.wait_for_members_sync(expected_count=3)
        yield client


class TestClusterMembers:
    """Test cluster membership functionality."""
    
    def test_members_response_structure(self, cluster: ClusterClient):
        """Test that /members endpoint returns correct structure."""
        for i, port in enumerate(HTTP_PORTS):
            result = cluster.query_members(port)
            
            assert result.get("success") is True, f"Expected success=true for node {NODES[i]}"
            assert "members" in result, f"Expected 'members' field for node {NODES[i]}"
    
    def test_members_ids(self, cluster: ClusterClient):
        """Test that members include expected node IDs."""
        expected_ids = {"1", "2", "3"}
        
        for i, port in enumerate(HTTP_PORTS):
            result = cluster.query_members(port)
            members = result.get("members", {})
            actual_ids = set(members.keys())
            
            assert actual_ids == expected_ids, (
                f"Node {NODES[i]}: Expected members {expected_ids}, got {actual_ids}"
            )
    
    def test_members_details(self, cluster: ClusterClient):
        """Test that member details are valid."""
        for i, port in enumerate(HTTP_PORTS):
            result = cluster.query_members(port)
            members = result.get("members", {})
            
            for node_id, node_info in members.items():
                assert "node_id" in node_info, f"Missing node_id for member {node_id}"
                assert "endpoint" in node_info, f"Missing endpoint for member {node_id}"
                assert node_info["node_id"] == int(node_id), (
                    f"node_id mismatch for member {node_id}"
                )
    
    def test_members_consistency_across_nodes(self, cluster: ClusterClient):
        """Test that all nodes report consistent membership."""
        all_members = []
        
        for port in HTTP_PORTS:
            result = cluster.query_members(port)
            all_members.append(result.get("members", {}))
        
        # Compare all nodes' membership with the first node
        first_members = all_members[0]
        for i, members in enumerate(all_members[1:], 1):
            assert first_members == members, (
                f"Membership mismatch between {NODES[0]} and {NODES[i]}"
            )


def main():
    """Main entry point for direct execution."""
    pytest.main([__file__, "-v"])


if __name__ == "__main__":
    main()
