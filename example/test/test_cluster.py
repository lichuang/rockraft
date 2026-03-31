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
import urllib.parse
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
    
    def set_value(self, port: int, key: str, value: str, timeout: int = 5) -> Dict[str, Any]:
        """Set a key-value pair via the /set endpoint."""
        url = f"http://127.0.0.1:{port}/set"
        data = json.dumps({"key": key, "value": value}).encode()
        headers = {"Content-Type": "application/json"}
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            raise RuntimeError(f"Failed to set value on port {port}: {e}")
    
    def get_value(self, port: int, key: str, timeout: int = 5) -> Dict[str, Any]:
        """Get a value by key via the /get endpoint."""
        url = f"http://127.0.0.1:{port}/get?key={urllib.parse.quote(key)}"
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            raise RuntimeError(f"Failed to get value on port {port}: {e}")
    
    def query_prefix(self, port: int, prefix: str, timeout: int = 5) -> Dict[str, Any]:
        """Scan keys by prefix via the /prefix endpoint."""
        url = f"http://127.0.0.1:{port}/prefix?prefix={urllib.parse.quote(prefix)}"
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            raise RuntimeError(f"Failed to query prefix on port {port}: {e}")
    
    def batch_write(self, port: int, operations: list, timeout: int = 10) -> Dict[str, Any]:
        """Batch write multiple operations atomically via the /batch_write endpoint.
        
        Args:
            port: HTTP port of the node
            operations: List of dicts with keys 'op' (set/delete), 'key', and optionally 'value'
            timeout: Request timeout in seconds
        
        Returns:
            Response dict with 'success' and 'message' fields
        """
        url = f"http://127.0.0.1:{port}/batch_write"
        data = json.dumps({"operations": operations}).encode()
        headers = {"Content-Type": "application/json"}
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            raise RuntimeError(f"Failed to batch write on port {port}: {e}")
    
    def txn(self, port: int, conditions: list, if_then: list, else_then: list = None, 
            return_previous: bool = False, timeout: int = 10) -> Dict[str, Any]:
        """Execute a transaction via the /txn endpoint.
        
        Args:
            port: HTTP port of the node
            conditions: List of condition dicts with 'key', 'op', and optionally 'value'
            if_then: List of operation dicts to execute if conditions are met
            else_then: List of operation dicts to execute if conditions are not met
            return_previous: Whether to return previous values
            timeout: Request timeout in seconds
        
        Returns:
            Response dict with 'success', 'branch', and 'prev_values' fields
        """
        url = f"http://127.0.0.1:{port}/txn"
        payload = {
            "conditions": conditions,
            "if_then": if_then,
            "else_then": else_then or [],
            "return_previous": return_previous,
        }
        data = json.dumps(payload).encode()
        headers = {"Content-Type": "application/json"}
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            raise RuntimeError(f"Failed to execute transaction on port {port}: {e}")
    
    def getset(self, port: int, key: str, value: str, timeout: int = 5) -> Dict[str, Any]:
        """Get old value and set new value atomically via the /getset endpoint.
        
        Args:
            port: HTTP port of the node
            key: Key to get and set
            value: New value to set
            timeout: Request timeout in seconds
        
        Returns:
            Response dict with 'success', 'key', 'old_value', and 'new_value' fields
        """
        url = f"http://127.0.0.1:{port}/getset"
        data = json.dumps({"key": key, "value": value}).encode()
        headers = {"Content-Type": "application/json"}
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            raise RuntimeError(f"Failed to getset on port {port}: {e}")
    
    def join_node(self, port: int, node_id: int, endpoint: str, timeout: int = 10) -> Dict[str, Any]:
        """Add a node to the cluster via the /join endpoint.
        
        Args:
            port: HTTP port of the node to send the join request to
            node_id: ID of the node to add
            endpoint: Raft endpoint of the new node (e.g. "127.0.0.1:7004")
            timeout: Request timeout in seconds
        
        Returns:
            Response dict with 'success' and 'message' fields
        """
        url = f"http://127.0.0.1:{port}/join"
        data = json.dumps({"node_id": node_id, "endpoint": endpoint}).encode()
        headers = {"Content-Type": "application/json"}
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except Exception as e:
            raise RuntimeError(f"Failed to join node {node_id} on port {port}: {e}")
    
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


class TestClusterData:
    """Test data read/write functionality."""
    
    def test_write_and_read_on_same_node(self, cluster: ClusterClient):
        """Test writing and reading on the same node."""
        test_key = "test_key_1"
        test_value = "test_value_1"
        
        # Write on node1
        result = cluster.set_value(HTTP_PORTS[0], test_key, test_value)
        assert result.get("success") is True, f"Write failed: {result}"
        
        # Read on the same node
        result = cluster.get_value(HTTP_PORTS[0], test_key)
        assert result.get("key") == test_key, f"Key mismatch: {result}"
        assert result.get("value") == test_value, f"Value mismatch: expected {test_value}, got {result.get('value')}"
    
    def test_write_and_read_across_nodes(self, cluster: ClusterClient):
        """Test writing on one node and reading from another."""
        test_key = "test_key_2"
        test_value = "test_value_2"
        
        # Write on node1
        result = cluster.set_value(HTTP_PORTS[0], test_key, test_value)
        assert result.get("success") is True, f"Write failed: {result}"
        
        # Read from node2 and node3
        for i, port in enumerate(HTTP_PORTS[1:], 1):
            result = cluster.get_value(port, test_key)
            assert result.get("key") == test_key, f"Key mismatch on {NODES[i]}: {result}"
            assert result.get("value") == test_value, (
                f"Value mismatch on {NODES[i]}: expected {test_value}, got {result.get('value')}"
            )
    
    def test_data_consistency_across_all_nodes(self, cluster: ClusterClient):
        """Test that written data is consistent across all nodes."""
        test_data = {
            "key_a": "value_a",
            "key_b": "value_b",
            "key_c": "value_c",
        }
        
        # Write all data on node1
        for key, value in test_data.items():
            result = cluster.set_value(HTTP_PORTS[0], key, value)
            assert result.get("success") is True, f"Failed to write {key}: {result}"
        
        # Read from all nodes and verify consistency
        for port in HTTP_PORTS:
            for key, expected_value in test_data.items():
                result = cluster.get_value(port, key)
                assert result.get("key") == key, f"Key mismatch on port {port}: {result}"
                assert result.get("value") == expected_value, (
                    f"Value mismatch for {key} on port {port}: expected {expected_value}, got {result.get('value')}"
                )
    
    def test_read_nonexistent_key(self, cluster: ClusterClient):
        """Test reading a key that doesn't exist."""
        test_key = "nonexistent_key_xyz"
        
        for port in HTTP_PORTS:
            result = cluster.get_value(port, test_key)
            assert result.get("key") == test_key, f"Key mismatch: {result}"
            assert result.get("value") is None, f"Expected None for non-existent key, got {result.get('value')}"
    
    def test_overwrite_existing_key(self, cluster: ClusterClient):
        """Test overwriting an existing key with a new value."""
        test_key = "overwrite_key"
        first_value = "first_value"
        second_value = "second_value"
        
        # Write initial value
        result = cluster.set_value(HTTP_PORTS[0], test_key, first_value)
        assert result.get("success") is True, f"Initial write failed: {result}"
        
        # Verify initial value
        result = cluster.get_value(HTTP_PORTS[0], test_key)
        assert result.get("value") == first_value, f"Initial value mismatch: {result}"
        
        # Overwrite with new value
        result = cluster.set_value(HTTP_PORTS[1], test_key, second_value)
        assert result.get("success") is True, f"Overwrite failed: {result}"
        
        # Verify new value from all nodes
        for port in HTTP_PORTS:
            result = cluster.get_value(port, test_key)
            assert result.get("value") == second_value, (
                f"Overwritten value mismatch on port {port}: expected {second_value}, got {result.get('value')}"
            )
    
    def test_write_special_characters(self, cluster: ClusterClient):
        """Test writing and reading values with special characters."""
        test_cases = [
            ("special_key_1", "Hello, 世界! 🌍"),
            ("special_key_2", "Line1\nLine2\nLine3"),
            ("special_key_3", "Tab\tSeparated\tValues"),
            ("special_key_4", "Quotes: \"single\" and 'double'"),
            ("special_key_5", "Symbols: !@#$%^&*()_+-=[]{}|;':\",./<>?"),
        ]
        
        for key, value in test_cases:
            # Write on a random node
            write_port = HTTP_PORTS[hash(key) % len(HTTP_PORTS)]
            result = cluster.set_value(write_port, key, value)
            assert result.get("success") is True, f"Failed to write {key}: {result}"
            
            # Read from all nodes
            for read_port in HTTP_PORTS:
                result = cluster.get_value(read_port, key)
                assert result.get("key") == key, f"Key mismatch on port {read_port}: {result}"
                assert result.get("value") == value, (
                    f"Value mismatch for {key} on port {read_port}: expected {repr(value)}, got {repr(result.get('value'))}"
                )
    
    def test_scan_prefix_basic(self, cluster: ClusterClient):
        """Test scanning keys by prefix on a single node."""
        prefix = "prefix_test_"
        test_data = {
            f"{prefix}key_1": "value_1",
            f"{prefix}key_2": "value_2",
            f"{prefix}key_3": "value_3",
            "other_key_1": "other_value_1",
            "other_key_2": "other_value_2",
        }
        
        # Write all data on node1
        for key, value in test_data.items():
            result = cluster.set_value(HTTP_PORTS[0], key, value)
            assert result.get("success") is True, f"Failed to write {key}: {result}"
        
        # Scan by prefix
        result = cluster.query_prefix(HTTP_PORTS[0], prefix)
        assert result.get("success") is True, f"Prefix scan failed: {result}"
        assert result.get("prefix") == prefix, f"Prefix mismatch: {result}"
        assert result.get("count") == 3, f"Expected 3 items, got {result.get('count')}"
        
        items = result.get("items", [])
        keys = {item["key"] for item in items}
        expected_keys = {f"{prefix}key_1", f"{prefix}key_2", f"{prefix}key_3"}
        assert keys == expected_keys, f"Keys mismatch: expected {expected_keys}, got {keys}"
    
    def test_scan_prefix_empty_result(self, cluster: ClusterClient):
        """Test scanning with a prefix that doesn't match any keys."""
        prefix = "nonexistent_prefix_"
        
        # Scan by prefix (no data written with this prefix)
        result = cluster.query_prefix(HTTP_PORTS[0], prefix)
        assert result.get("success") is True, f"Prefix scan failed: {result}"
        assert result.get("prefix") == prefix, f"Prefix mismatch: {result}"
        assert result.get("count") == 0, f"Expected 0 items, got {result.get('count')}"
        assert result.get("items") == [], f"Expected empty items list: {result}"
    
    def test_scan_prefix_across_nodes(self, cluster: ClusterClient):
        """Test scanning keys by prefix from different nodes."""
        prefix = "cross_node_prefix_"
        test_data = {
            f"{prefix}key_a": "value_a",
            f"{prefix}key_b": "value_b",
            f"{prefix}key_c": "value_c",
        }
        
        # Write data on node1
        for key, value in test_data.items():
            result = cluster.set_value(HTTP_PORTS[0], key, value)
            assert result.get("success") is True, f"Failed to write {key}: {result}"
        
        # Scan from all nodes and verify consistency
        for port in HTTP_PORTS:
            result = cluster.query_prefix(port, prefix)
            assert result.get("success") is True, f"Prefix scan failed on port {port}: {result}"
            assert result.get("count") == 3, f"Expected 3 items on port {port}, got {result.get('count')}"
            
            items = result.get("items", [])
            keys = {item["key"] for item in items}
            expected_keys = set(test_data.keys())
            assert keys == expected_keys, f"Keys mismatch on port {port}: expected {expected_keys}, got {keys}"
            
            # Verify values are correct
            for item in items:
                expected_value = test_data[item["key"]]
                assert item["value"] == expected_value, (
                    f"Value mismatch for {item['key']} on port {port}: expected {expected_value}, got {item['value']}"
                )
    
    def test_scan_prefix_nested_prefix(self, cluster: ClusterClient):
        """Test scanning with nested prefixes."""
        # Create keys with hierarchical structure
        test_data = {
            "app:config:db:host": "localhost",
            "app:config:db:port": "5432",
            "app:config:db:name": "mydb",
            "app:config:cache:host": "redis",
            "app:config:cache:port": "6379",
            "app:users:admin:name": "Admin",
            "app:users:admin:role": "superuser",
        }
        
        # Write all data on node1
        for key, value in test_data.items():
            result = cluster.set_value(HTTP_PORTS[0], key, value)
            assert result.get("success") is True, f"Failed to write {key}: {result}"
        
        # Test scanning with different prefix levels
        # Level 1: "app:"
        result = cluster.query_prefix(HTTP_PORTS[0], "app:")
        assert result.get("count") == 7, f"Expected 7 items for 'app:', got {result.get('count')}"
        
        # Level 2: "app:config:"
        result = cluster.query_prefix(HTTP_PORTS[0], "app:config:")
        assert result.get("count") == 5, f"Expected 5 items for 'app:config:', got {result.get('count')}"
        
        # Level 3: "app:config:db:"
        result = cluster.query_prefix(HTTP_PORTS[0], "app:config:db:")
        assert result.get("count") == 3, f"Expected 3 items for 'app:config:db:', got {result.get('count')}"
        
        # Level 3: "app:config:cache:"
        result = cluster.query_prefix(HTTP_PORTS[0], "app:config:cache:")
        assert result.get("count") == 2, f"Expected 2 items for 'app:config:cache:', got {result.get('count')}"
        
        # Level 2: "app:users:"
        result = cluster.query_prefix(HTTP_PORTS[0], "app:users:")
        assert result.get("count") == 2, f"Expected 2 items for 'app:users:', got {result.get('count')}"


class TestClusterRestart:
    """Test cluster restart and data persistence functionality."""
    
    def test_restart_cluster_with_data_persistence(self):
        """
        Test the full lifecycle: start -> modify data -> stop -> restart -> verify members and data.
        
        This test verifies:
        1. Cluster starts successfully
        2. Data can be written to the cluster
        3. Cluster stops gracefully
        4. Cluster restarts successfully
        5. All nodes report consistent membership
        6. Previously written data persists after restart
        """
        cluster_mgr = ClusterManager(BASE_DIR, START_SCRIPT)
        
        # Phase 1: Start the cluster
        print("\n" + "="*60)
        print("PHASE 1: Starting cluster for the first time...")
        print("="*60)
        cluster_mgr.clean()
        cluster_mgr.build()
        cluster_mgr.start()
        
        try:
            client = ClusterClient(HTTP_PORTS)
            client.wait_for_nodes()
            client.wait_for_members_sync(expected_count=3)
            print("[PHASE 1] Cluster started successfully")
            
            # Phase 2: Write test data
            print("\n" + "="*60)
            print("PHASE 2: Writing test data...")
            print("="*60)
            test_data = {
                "persist_key_1": "persist_value_1",
                "persist_key_2": "persist_value_2",
                "persist_key_3": "persist_value_3",
            }
            
            for key, value in test_data.items():
                result = client.set_value(HTTP_PORTS[0], key, value)
                assert result.get("success") is True, f"Failed to write {key}: {result}"
                print(f"  Written: {key} = {value}")
            print("[PHASE 2] Data written successfully")
            
            # Verify data is readable before stop
            print("\n  Verifying data before stop...")
            for key, expected_value in test_data.items():
                result = client.get_value(HTTP_PORTS[0], key)
                assert result.get("value") == expected_value, (
                    f"Value mismatch for {key}: expected {expected_value}, got {result.get('value')}"
                )
            print("  Data verified before stop")
            
            # Verify members before stop
            print("\n  Verifying members before stop...")
            expected_ids = {"1", "2", "3"}
            for port in HTTP_PORTS:
                result = client.query_members(port)
                members = result.get("members", {})
                actual_ids = set(members.keys())
                assert actual_ids == expected_ids, (
                    f"Membership mismatch before stop: expected {expected_ids}, got {actual_ids}"
                )
            print("  Members verified before stop")
            
        finally:
            # Phase 3: Stop the cluster
            print("\n" + "="*60)
            print("PHASE 3: Stopping cluster...")
            print("="*60)
            cluster_mgr.stop()
            print("[PHASE 3] Cluster stopped")
            
            # Wait a bit to ensure all processes are terminated
            time.sleep(2)
        
        # Phase 4: Restart the cluster
        print("\n" + "="*60)
        print("PHASE 4: Restarting cluster...")
        print("="*60)
        cluster_mgr.start()
        
        try:
            client = ClusterClient(HTTP_PORTS)
            client.wait_for_nodes()
            client.wait_for_members_sync(expected_count=3)
            print("[PHASE 4] Cluster restarted successfully")
            
            # Phase 5: Verify members after restart
            print("\n" + "="*60)
            print("PHASE 5: Verifying members after restart...")
            print("="*60)
            for i, port in enumerate(HTTP_PORTS):
                result = client.query_members(port)
                
                assert result.get("success") is True, f"Expected success=true for {NODES[i]}"
                assert "members" in result, f"Expected 'members' field for {NODES[i]}"
                
                members = result.get("members", {})
                actual_ids = set(members.keys())
                
                assert actual_ids == expected_ids, (
                    f"{NODES[i]}: Expected members {expected_ids}, got {actual_ids}"
                )
                
                # Verify member details
                for node_id, node_info in members.items():
                    assert "node_id" in node_info, f"Missing node_id for member {node_id}"
                    assert "endpoint" in node_info, f"Missing endpoint for member {node_id}"
                    assert node_info["node_id"] == int(node_id), (
                        f"node_id mismatch for member {node_id}"
                    )
                
                print(f"  {NODES[i]} (port {port}): members = {actual_ids}")
            
            # Verify membership consistency across nodes
            all_members = []
            for port in HTTP_PORTS:
                result = client.query_members(port)
                all_members.append(result.get("members", {}))
            
            first_members = all_members[0]
            for i, members in enumerate(all_members[1:], 1):
                assert first_members == members, (
                    f"Membership mismatch after restart between {NODES[0]} and {NODES[i]}"
                )
            print("[PHASE 5] All members verified and consistent")
            
            # Phase 6: Verify persisted data
            print("\n" + "="*60)
            print("PHASE 6: Verifying persisted data...")
            print("="*60)
            
            # Read data from all nodes and verify
            for port in HTTP_PORTS:
                print(f"\n  Reading from port {port}:")
                for key, expected_value in test_data.items():
                    result = client.get_value(port, key)
                    assert result.get("key") == key, f"Key mismatch on port {port}: {result}"
                    assert result.get("value") == expected_value, (
                        f"Data persistence failed for {key} on port {port}: "
                        f"expected {expected_value}, got {result.get('value')}"
                    )
                    print(f"    {key} = {result.get('value')}")
            
            print("[PHASE 6] All data persisted correctly after restart")
            
            print("\n" + "="*60)
            print("TEST PASSED: Cluster restart with data persistence")
            print("="*60)
            
        finally:
            # Cleanup
            print("\n[CLEANUP] Stopping cluster...")
            cluster_mgr.stop()
    
    def test_restart_cluster_with_leader_failover(self):
        """
        Test cluster restart and leader election after restart.
        
        This test verifies:
        1. Cluster starts and elects a leader
        2. Data is written to the leader
        3. Cluster stops gracefully
        4. Cluster restarts successfully
        5. A new leader is elected
        6. New data can be written after restart
        7. Both old and new data are accessible
        """
        cluster_mgr = ClusterManager(BASE_DIR, START_SCRIPT)
        
        # Phase 1: Start cluster and write initial data
        print("\n" + "="*60)
        print("PHASE 1: Starting cluster and writing initial data...")
        print("="*60)
        cluster_mgr.clean()
        cluster_mgr.build()
        cluster_mgr.start()
        
        try:
            client = ClusterClient(HTTP_PORTS)
            client.wait_for_nodes()
            client.wait_for_members_sync(expected_count=3)
            
            initial_data = {
                "initial_key_1": "initial_value_1",
                "initial_key_2": "initial_value_2",
            }
            
            for key, value in initial_data.items():
                result = client.set_value(HTTP_PORTS[0], key, value)
                assert result.get("success") is True, f"Failed to write {key}: {result}"
            print("[PHASE 1] Initial data written")
            
        finally:
            # Stop cluster
            print("\nStopping cluster...")
            cluster_mgr.stop()
            time.sleep(2)
        
        # Phase 2: Restart cluster
        print("\n" + "="*60)
        print("PHASE 2: Restarting cluster...")
        print("="*60)
        cluster_mgr.start()
        
        try:
            client = ClusterClient(HTTP_PORTS)
            client.wait_for_nodes()
            client.wait_for_members_sync(expected_count=3)
            
            # Find the leader
            leader_port = None
            for port in HTTP_PORTS:
                url = f"http://127.0.0.1:{port}/health"
                try:
                    with urllib.request.urlopen(url, timeout=2) as response:
                        health = json.loads(response.read().decode())
                        if health.get("is_leader"):
                            leader_port = port
                            print(f"  Leader found on port {port}")
                            break
                except Exception:
                    pass
            
            assert leader_port is not None, "No leader found after restart"
            
            # Phase 3: Write new data after restart
            print("\n" + "="*60)
            print("PHASE 3: Writing new data after restart...")
            print("="*60)
            new_data = {
                "new_key_1": "new_value_1",
                "new_key_2": "new_value_2",
            }
            
            for key, value in new_data.items():
                result = client.set_value(leader_port, key, value)
                assert result.get("success") is True, f"Failed to write {key} after restart: {result}"
            print("[PHASE 3] New data written")
            
            # Phase 4: Verify all data (initial + new)
            print("\n" + "="*60)
            print("PHASE 4: Verifying all data...")
            print("="*60)
            all_data = {**initial_data, **new_data}
            
            for port in HTTP_PORTS:
                print(f"\n  Reading from port {port}:")
                for key, expected_value in all_data.items():
                    result = client.get_value(port, key)
                    assert result.get("value") == expected_value, (
                        f"Data mismatch for {key} on port {port}: "
                        f"expected {expected_value}, got {result.get('value')}"
                    )
                    print(f"    {key} = {result.get('value')}")
            
            print("[PHASE 4] All data verified successfully")
            
            print("\n" + "="*60)
            print("TEST PASSED: Cluster restart with leader failover")
            print("="*60)
            
        finally:
            # Cleanup - stop the test cluster
            print("\n[CLEANUP] Stopping test cluster...")
            cluster_mgr.stop()
            
            # Restart the original cluster for subsequent tests
            print("\n[RESTART] Restarting original cluster for subsequent tests...")
            cluster_mgr_start = ClusterManager(BASE_DIR, START_SCRIPT)
            cluster_mgr_start.start()
            
            # Wait for nodes to be ready
            client_restart = ClusterClient(HTTP_PORTS)
            client_restart.wait_for_nodes()
            client_restart.wait_for_members_sync(expected_count=3)
            print("[RESTART] Cluster restarted successfully")


class TestTransaction:
    """Test transaction functionality with conditions."""
    
    def test_txn_basic_if_then(self, cluster: ClusterClient):
        """Test basic transaction with conditions met (if_then branch)."""
        # First set a key that we'll check in the condition
        cluster.set_value(HTTP_PORTS[0], "txn_test_key", "expected_value")
        
        # Execute transaction: if key equals "expected_value", update it
        conditions = [
            {"key": "txn_test_key", "op": "equal", "value": "expected_value"}
        ]
        if_then = [
            {"op": "set", "key": "txn_test_key", "value": "updated_value"}
        ]
        
        result = cluster.txn(HTTP_PORTS[0], conditions, if_then)
        assert result.get("success") is True, f"Transaction failed: {result}"
        assert result.get("branch") is True, f"Expected if_then branch, got: {result}"
        
        # Verify the key was updated
        result = cluster.get_value(HTTP_PORTS[0], "txn_test_key")
        assert result.get("value") == "updated_value", f"Value not updated: {result}"
    
    def test_txn_else_then_branch(self, cluster: ClusterClient):
        """Test transaction with conditions not met (else_then branch)."""
        # Set a key with a value that won't match the condition
        cluster.set_value(HTTP_PORTS[0], "txn_else_key", "actual_value")
        
        # Execute transaction: if key equals "wrong_value", set to "if_value"
        # Otherwise, set to "else_value"
        conditions = [
            {"key": "txn_else_key", "op": "equal", "value": "wrong_value"}
        ]
        if_then = [
            {"op": "set", "key": "txn_else_key", "value": "if_value"}
        ]
        else_then = [
            {"op": "set", "key": "txn_else_key", "value": "else_value"}
        ]
        
        result = cluster.txn(HTTP_PORTS[0], conditions, if_then, else_then)
        assert result.get("success") is True, f"Transaction failed: {result}"
        assert result.get("branch") is False, f"Expected else_then branch, got: {result}"
        
        # Verify the else value was set
        result = cluster.get_value(HTTP_PORTS[0], "txn_else_key")
        assert result.get("value") == "else_value", f"Else value not set: {result}"
    
    def test_txn_return_previous_values(self, cluster: ClusterClient):
        """Test transaction returning previous values."""
        # Set initial value
        cluster.set_value(HTTP_PORTS[0], "txn_prev_key", "old_value")
        
        conditions = [
            {"key": "txn_prev_key", "op": "exists"}
        ]
        if_then = [
            {"op": "set", "key": "txn_prev_key", "value": "new_value"}
        ]
        
        result = cluster.txn(HTTP_PORTS[0], conditions, if_then, return_previous=True)
        assert result.get("success") is True, f"Transaction failed: {result}"
        assert result.get("branch") is True, f"Expected if_then branch: {result}"
        
        # Check previous values
        prev_values = result.get("prev_values", [])
        assert len(prev_values) == 1, f"Expected 1 prev_value, got: {prev_values}"
        assert prev_values[0] == "old_value", f"Expected old_value, got: {prev_values[0]}"
    
    def test_txn_exists_condition(self, cluster: ClusterClient):
        """Test transaction with exists condition."""
        # Set a key
        cluster.set_value(HTTP_PORTS[0], "txn_exists_key", "some_value")
        
        conditions = [
            {"key": "txn_exists_key", "op": "exists"}
        ]
        if_then = [
            {"op": "set", "key": "txn_exists_result", "value": "key_exists"}
        ]
        else_then = [
            {"op": "set", "key": "txn_exists_result", "value": "key_not_exists"}
        ]
        
        result = cluster.txn(HTTP_PORTS[0], conditions, if_then, else_then)
        assert result.get("success") is True, f"Transaction failed: {result}"
        assert result.get("branch") is True, f"Expected if_then branch: {result}"
        
        result = cluster.get_value(HTTP_PORTS[0], "txn_exists_result")
        assert result.get("value") == "key_exists", f"Unexpected result: {result}"
    
    def test_txn_not_exists_condition(self, cluster: ClusterClient):
        """Test transaction with not_exists condition."""
        # Ensure key doesn't exist
        conditions = [
            {"key": "txn_notexists_key", "op": "not_exists"}
        ]
        if_then = [
            {"op": "set", "key": "txn_notexists_key", "value": "created_value"}
        ]
        else_then = [
            {"op": "set", "key": "txn_notexists_key", "value": "wrong_value"}
        ]
        
        result = cluster.txn(HTTP_PORTS[0], conditions, if_then, else_then)
        assert result.get("success") is True, f"Transaction failed: {result}"
        assert result.get("branch") is True, f"Expected if_then branch: {result}"
        
        result = cluster.get_value(HTTP_PORTS[0], "txn_notexists_key")
        assert result.get("value") == "created_value", f"Key not created: {result}"
    
    def test_txn_multiple_conditions(self, cluster: ClusterClient):
        """Test transaction with multiple conditions (AND logic)."""
        # Set two keys
        cluster.set_value(HTTP_PORTS[0], "txn_multi_key1", "value1")
        cluster.set_value(HTTP_PORTS[0], "txn_multi_key2", "value2")
        
        # Both conditions must be met
        conditions = [
            {"key": "txn_multi_key1", "op": "equal", "value": "value1"},
            {"key": "txn_multi_key2", "op": "equal", "value": "value2"}
        ]
        if_then = [
            {"op": "set", "key": "txn_multi_result", "value": "both_met"}
        ]
        else_then = [
            {"op": "set", "key": "txn_multi_result", "value": "not_met"}
        ]
        
        result = cluster.txn(HTTP_PORTS[0], conditions, if_then, else_then)
        assert result.get("success") is True, f"Transaction failed: {result}"
        assert result.get("branch") is True, f"Expected if_then branch: {result}"
        
        result = cluster.get_value(HTTP_PORTS[0], "txn_multi_result")
        assert result.get("value") == "both_met", f"Unexpected result: {result}"
    
    def test_txn_delete_operation(self, cluster: ClusterClient):
        """Test transaction with delete operation."""
        # Set a key to delete
        cluster.set_value(HTTP_PORTS[0], "txn_delete_key", "to_be_deleted")
        
        conditions = [
            {"key": "txn_delete_key", "op": "exists"}
        ]
        if_then = [
            {"op": "delete", "key": "txn_delete_key"}
        ]
        
        result = cluster.txn(HTTP_PORTS[0], conditions, if_then, return_previous=True)
        assert result.get("success") is True, f"Transaction failed: {result}"
        
        # Verify the key was deleted
        result = cluster.get_value(HTTP_PORTS[0], "txn_delete_key")
        assert result.get("value") is None, f"Key not deleted: {result}"
    
    def test_txn_multiple_operations(self, cluster: ClusterClient):
        """Test transaction with multiple operations in if_then."""
        cluster.set_value(HTTP_PORTS[0], "txn_multi_cond", "trigger")
        
        conditions = [
            {"key": "txn_multi_cond", "op": "equal", "value": "trigger"}
        ]
        if_then = [
            {"op": "set", "key": "txn_op_key1", "value": "value1"},
            {"op": "set", "key": "txn_op_key2", "value": "value2"},
            {"op": "set", "key": "txn_op_key3", "value": "value3"}
        ]
        
        result = cluster.txn(HTTP_PORTS[0], conditions, if_then)
        assert result.get("success") is True, f"Transaction failed: {result}"
        
        # Verify all keys were set
        for i in range(1, 4):
            result = cluster.get_value(HTTP_PORTS[0], f"txn_op_key{i}")
            assert result.get("value") == f"value{i}", f"Key {i} not set: {result}"
    
    def test_txn_consistency_across_nodes(self, cluster: ClusterClient):
        """Test that transaction results are consistent across all nodes."""
        # Set initial value
        cluster.set_value(HTTP_PORTS[0], "txn_consistency_key", "initial")
        
        conditions = [
            {"key": "txn_consistency_key", "op": "equal", "value": "initial"}
        ]
        if_then = [
            {"op": "set", "key": "txn_consistency_key", "value": "updated"}
        ]
        
        result = cluster.txn(HTTP_PORTS[0], conditions, if_then)
        assert result.get("success") is True, f"Transaction failed: {result}"
        
        # Verify from all nodes
        for port in HTTP_PORTS:
            result = cluster.get_value(port, "txn_consistency_key")
            assert result.get("value") == "updated", (
                f"Inconsistent value on port {port}: {result}"
            )


class TestGetSet:
    """Test getset functionality."""
    
    def test_getset_new_key(self, cluster: ClusterClient):
        """Test getset on a non-existent key."""
        result = cluster.getset(HTTP_PORTS[0], "getset_new_key", "new_value")
        assert result.get("success") is True, f"Getset failed: {result}"
        assert result.get("key") == "getset_new_key", f"Key mismatch: {result}"
        assert result.get("old_value") is None, f"Expected None for new key: {result}"
        assert result.get("new_value") == "new_value", f"New value mismatch: {result}"
        
        # Verify the key was created
        result = cluster.get_value(HTTP_PORTS[0], "getset_new_key")
        assert result.get("value") == "new_value", f"Key not created: {result}"
    
    def test_getset_existing_key(self, cluster: ClusterClient):
        """Test getset on an existing key."""
        # First create the key
        cluster.set_value(HTTP_PORTS[0], "getset_existing_key", "old_value")
        
        # Now getset it
        result = cluster.getset(HTTP_PORTS[0], "getset_existing_key", "new_value")
        assert result.get("success") is True, f"Getset failed: {result}"
        assert result.get("old_value") == "old_value", f"Old value mismatch: {result}"
        assert result.get("new_value") == "new_value", f"New value mismatch: {result}"
        
        # Verify the new value
        result = cluster.get_value(HTTP_PORTS[0], "getset_existing_key")
        assert result.get("value") == "new_value", f"Value not updated: {result}"
    
    def test_getset_overwrite_multiple_times(self, cluster: ClusterClient):
        """Test getset overwriting a key multiple times."""
        # First set
        cluster.set_value(HTTP_PORTS[0], "getset_multi_key", "value1")
        
        # Second set
        result = cluster.getset(HTTP_PORTS[0], "getset_multi_key", "value2")
        assert result.get("old_value") == "value1", f"First getset failed: {result}"
        
        # Third set
        result = cluster.getset(HTTP_PORTS[0], "getset_multi_key", "value3")
        assert result.get("old_value") == "value2", f"Second getset failed: {result}"
        
        # Verify final value
        result = cluster.get_value(HTTP_PORTS[0], "getset_multi_key")
        assert result.get("value") == "value3", f"Final value mismatch: {result}"
    
    def test_getset_consistency_across_nodes(self, cluster: ClusterClient):
        """Test that getset is consistent across all nodes."""
        # Create initial value
        cluster.set_value(HTTP_PORTS[0], "getset_consistency_key", "initial")
        
        # Getset from node1
        result = cluster.getset(HTTP_PORTS[0], "getset_consistency_key", "updated")
        assert result.get("success") is True, f"Getset failed: {result}"
        
        # Verify from all nodes
        for port in HTTP_PORTS:
            result = cluster.get_value(port, "getset_consistency_key")
            assert result.get("value") == "updated", (
                f"Inconsistent value on port {port}: {result}"
            )
    
    def test_getset_special_characters(self, cluster: ClusterClient):
        """Test getset with special characters in value."""
        test_cases = [
            ("getset_special_1", "Hello, 世界! 🌍"),
            ("getset_special_2", "Line1\nLine2\nLine3"),
            ("getset_special_3", "Tab\tSeparated\tValues"),
            ("getset_special_4", 'Quotes: "single" and \'double\''),
        ]
        
        for key, value in test_cases:
            # Set initial value
            cluster.set_value(HTTP_PORTS[0], key, "initial")
            
            # Getset with special characters
            result = cluster.getset(HTTP_PORTS[0], key, value)
            assert result.get("success") is True, f"Getset failed for {key}: {result}"
            assert result.get("old_value") == "initial", f"Old value mismatch for {key}: {result}"
            
            # Verify new value
            result = cluster.get_value(HTTP_PORTS[0], key)
            assert result.get("value") == value, (
                f"Value mismatch for {key}: expected {repr(value)}, got {repr(result.get('value'))}"
            )


class TestBatchWrite:
    """Test batch write functionality."""
    
    def test_batch_write_multiple_sets(self, cluster: ClusterClient):
        """Test batch writing multiple set operations."""
        operations = [
            {"op": "set", "key": "batch_key_1", "value": "batch_value_1"},
            {"op": "set", "key": "batch_key_2", "value": "batch_value_2"},
            {"op": "set", "key": "batch_key_3", "value": "batch_value_3"},
        ]
        
        # Execute batch write on node1
        result = cluster.batch_write(HTTP_PORTS[0], operations)
        assert result.get("success") is True, f"Batch write failed: {result}"
        assert "3 operations" in result.get("message", ""), f"Unexpected message: {result}"
        
        # Verify all keys are readable from all nodes
        expected_data = {
            "batch_key_1": "batch_value_1",
            "batch_key_2": "batch_value_2",
            "batch_key_3": "batch_value_3",
        }
        
        for port in HTTP_PORTS:
            for key, expected_value in expected_data.items():
                result = cluster.get_value(port, key)
                assert result.get("key") == key, f"Key mismatch on port {port}: {result}"
                assert result.get("value") == expected_value, (
                    f"Value mismatch for {key} on port {port}: expected {expected_value}, got {result.get('value')}"
                )
    
    def test_batch_write_mixed_operations(self, cluster: ClusterClient):
        """Test batch write with both set and delete operations."""
        # First, set some initial values
        initial_ops = [
            {"op": "set", "key": "mixed_key_1", "value": "initial_value_1"},
            {"op": "set", "key": "mixed_key_2", "value": "initial_value_2"},
            {"op": "set", "key": "mixed_key_3", "value": "initial_value_3"},
        ]
        
        result = cluster.batch_write(HTTP_PORTS[0], initial_ops)
        assert result.get("success") is True, f"Initial batch write failed: {result}"
        
        # Verify initial values
        for i in range(1, 4):
            result = cluster.get_value(HTTP_PORTS[0], f"mixed_key_{i}")
            assert result.get("value") == f"initial_value_{i}"
        
        # Now perform mixed batch: update key_1, delete key_2, set key_4
        mixed_ops = [
            {"op": "set", "key": "mixed_key_1", "value": "updated_value_1"},
            {"op": "delete", "key": "mixed_key_2"},
            {"op": "set", "key": "mixed_key_4", "value": "new_value_4"},
        ]
        
        result = cluster.batch_write(HTTP_PORTS[1], mixed_ops)  # Write from different node
        assert result.get("success") is True, f"Mixed batch write failed: {result}"
        
        # Verify results from all nodes
        for port in HTTP_PORTS:
            # Key 1 should be updated
            result = cluster.get_value(port, "mixed_key_1")
            assert result.get("value") == "updated_value_1", (
                f"Key 1 not updated on port {port}: {result}"
            )
            
            # Key 2 should be deleted
            result = cluster.get_value(port, "mixed_key_2")
            assert result.get("value") is None, (
                f"Key 2 not deleted on port {port}: {result}"
            )
            
            # Key 3 should remain unchanged
            result = cluster.get_value(port, "mixed_key_3")
            assert result.get("value") == "initial_value_3", (
                f"Key 3 changed unexpectedly on port {port}: {result}"
            )
            
            # Key 4 should be new
            result = cluster.get_value(port, "mixed_key_4")
            assert result.get("value") == "new_value_4", (
                f"Key 4 not set on port {port}: {result}"
            )
    
    def test_batch_write_empty_operations(self, cluster: ClusterClient):
        """Test batch write with empty operations list."""
        result = cluster.batch_write(HTTP_PORTS[0], [])
        assert result.get("success") is True, f"Empty batch write failed: {result}"
        assert "No operations" in result.get("message", ""), f"Unexpected message: {result}"
    
    def test_batch_write_single_operation(self, cluster: ClusterClient):
        """Test batch write with a single operation."""
        operations = [
            {"op": "set", "key": "single_batch_key", "value": "single_batch_value"},
        ]
        
        result = cluster.batch_write(HTTP_PORTS[0], operations)
        assert result.get("success") is True, f"Single batch write failed: {result}"
        
        # Verify from all nodes
        for port in HTTP_PORTS:
            result = cluster.get_value(port, "single_batch_key")
            assert result.get("value") == "single_batch_value", (
                f"Value mismatch on port {port}: {result}"
            )
    
    def test_batch_write_special_characters(self, cluster: ClusterClient):
        """Test batch write with special characters in keys and values."""
        operations = [
            {"op": "set", "key": "batch:special:1", "value": "Hello, 世界! 🌍"},
            {"op": "set", "key": "batch:special:2", "value": "Line1\nLine2\nLine3"},
            {"op": "set", "key": "batch:special:3", "value": "Tab\tSeparated\tValues"},
            {"op": "delete", "key": "batch:special:nonexistent"},
        ]
        
        result = cluster.batch_write(HTTP_PORTS[0], operations)
        assert result.get("success") is True, f"Special chars batch write failed: {result}"
        
        # Verify set operations
        expected_values = {
            "batch:special:1": "Hello, 世界! 🌍",
            "batch:special:2": "Line1\nLine2\nLine3",
            "batch:special:3": "Tab\tSeparated\tValues",
        }
        
        for port in HTTP_PORTS:
            for key, expected_value in expected_values.items():
                result = cluster.get_value(port, key)
                assert result.get("value") == expected_value, (
                    f"Value mismatch for {key} on port {port}: expected {repr(expected_value)}, got {repr(result.get('value'))}"
                )
    
    def test_batch_write_consistency_across_nodes(self, cluster: ClusterClient):
        """Test that batch write is consistent across all nodes."""
        operations = []
        expected_data = {}
        
        # Create 10 key-value pairs
        for i in range(10):
            key = f"consistency_key_{i}"
            value = f"consistency_value_{i}"
            operations.append({"op": "set", "key": key, "value": value})
            expected_data[key] = value
        
        # Execute batch write on node1
        result = cluster.batch_write(HTTP_PORTS[0], operations)
        assert result.get("success") is True, f"Consistency batch write failed: {result}"
        
        # Verify all nodes have the same data
        for port in HTTP_PORTS:
            for key, expected_value in expected_data.items():
                result = cluster.get_value(port, key)
                assert result.get("value") == expected_value, (
                    f"Data inconsistency for {key} on port {port}: expected {expected_value}, got {result.get('value')}"
                )
    
    def test_batch_write_large_batch(self, cluster: ClusterClient):
        """Test batch write with a large number of operations."""
        operations = []
        expected_data = {}
        
        # Create 50 key-value pairs
        for i in range(50):
            key = f"large_batch_key_{i:03d}"
            value = f"large_batch_value_{i:03d}_" + "x" * 100  # Add some padding
            operations.append({"op": "set", "key": key, "value": value})
            expected_data[key] = value
        
        # Execute batch write
        result = cluster.batch_write(HTTP_PORTS[0], operations)
        assert result.get("success") is True, f"Large batch write failed: {result}"
        assert "50 operations" in result.get("message", ""), f"Unexpected message: {result}"
        
        # Verify a sample of the data from all nodes
        sample_keys = list(expected_data.keys())[:10]
        for port in HTTP_PORTS:
            for key in sample_keys:
                result = cluster.get_value(port, key)
                assert result.get("value") == expected_data[key], (
                    f"Value mismatch for {key} on port {port}"
                )
        
        # Verify count using prefix scan
        result = cluster.query_prefix(HTTP_PORTS[0], "large_batch_key_")
        assert result.get("count") == 50, f"Expected 50 items, got {result.get('count')}"


class TestJoin:
    """Test join node functionality."""

    def test_join_already_existing_node(self, cluster: ClusterClient):
        """Test joining a node that is already in the cluster returns success."""
        result = cluster.join_node(HTTP_PORTS[0], 1, "127.0.0.1:7001")
        assert result.get("success") is True, f"Join existing node failed: {result}"

    def test_join_with_invalid_endpoint(self, cluster: ClusterClient):
        """Test joining with an invalid endpoint returns an error."""
        try:
            cluster.join_node(HTTP_PORTS[0], 99, "invalid_endpoint")
            assert False, "Expected an error for invalid endpoint"
        except RuntimeError:
            pass

    def test_join_consistency_across_nodes(self, cluster: ClusterClient):
        """Test that join request forwarded to any node succeeds."""
        for i, port in enumerate(HTTP_PORTS):
            result = cluster.join_node(port, 2, "127.0.0.1:7002")
            assert result.get("success") is True, (
                f"Join via node {NODES[i]} (port {port}) failed: {result}"
            )


class TestConcurrency:
    def test_concurrent_writes_same_key(self, cluster: ClusterClient):
        """Test concurrent writes to the same key from multiple threads."""
        import concurrent.futures
        import threading
        
        key = "concurrent_same_key"
        num_threads = 10
        num_writes_per_thread = 5
        values_written = []
        lock = threading.Lock()
        errors = []
        
        def write_value(thread_id: int, write_num: int):
            try:
                value = f"thread_{thread_id}_write_{write_num}"
                port = HTTP_PORTS[thread_id % len(HTTP_PORTS)]
                result = cluster.set_value(port, key, value)
                if result.get("success"):
                    with lock:
                        values_written.append(value)
                else:
                    with lock:
                        errors.append(f"Write failed: thread={thread_id}, write={write_num}")
            except Exception as e:
                with lock:
                    errors.append(f"Exception in thread={thread_id}: {e}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for thread_id in range(num_threads):
                for write_num in range(num_writes_per_thread):
                    futures.append(executor.submit(write_value, thread_id, write_num))
            concurrent.futures.wait(futures)
        
        assert len(errors) == 0, f"Errors during concurrent writes: {errors}"
        assert len(values_written) > 0, "No writes succeeded"
        
        final_values = []
        for port in HTTP_PORTS:
            result = cluster.get_value(port, key)
            final_values.append(result.get("value"))
        
        assert len(set(final_values)) == 1, f"Nodes have inconsistent values: {final_values}"
        
        final_value = final_values[0]
        assert final_value in values_written, (
            f"Final value '{final_value}' not in written values"
        )
        
        print(f"[PASS] Concurrent writes to same key: {len(values_written)} writes succeeded, final value: {final_value}")
    
    def test_concurrent_writes_different_keys(self, cluster: ClusterClient):
        import concurrent.futures
        
        num_threads = 20
        keys_written = {}
        errors = []
        
        def write_unique_key(thread_id: int):
            try:
                key = f"concurrent_diff_key_{thread_id}"
                value = f"concurrent_diff_value_{thread_id}"
                port = HTTP_PORTS[thread_id % len(HTTP_PORTS)]
                result = cluster.set_value(port, key, value)
                if result.get("success"):
                    return (key, value, None)
                else:
                    return (key, value, f"Write failed: {result}")
            except Exception as e:
                return (f"key_{thread_id}", None, str(e))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(write_unique_key, i) for i in range(num_threads)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        for key, value, error in results:
            if error:
                errors.append(error)
            elif value:
                keys_written[key] = value
        
        assert len(errors) == 0, f"Errors during concurrent writes: {errors}"
        assert len(keys_written) == num_threads, f"Expected {num_threads} keys, got {len(keys_written)}"
        
        for port in HTTP_PORTS:
            for key, expected_value in keys_written.items():
                result = cluster.get_value(port, key)
                assert result.get("value") == expected_value, (
                    f"Value mismatch for {key} on port {port}: expected {expected_value}, got {result.get('value')}"
                )
        
        print(f"[PASS] Concurrent writes to different keys: {len(keys_written)} keys written")
    
    def test_concurrent_read_write_mixed(self, cluster: ClusterClient):
        import concurrent.futures
        import random
        
        key = "concurrent_rw_key"
        initial_value = "initial_value"
        num_iterations = 50
        
        cluster.set_value(HTTP_PORTS[0], key, initial_value)
        
        read_values = []
        write_count = 0
        errors = []
        
        def read_operation(op_id: int):
            nonlocal read_values
            try:
                port = HTTP_PORTS[op_id % len(HTTP_PORTS)]
                result = cluster.get_value(port, key)
                if result.get("key") == key:
                    read_values.append(result.get("value"))
                else:
                    errors.append(f"Read returned wrong key: {result}")
            except Exception as e:
                errors.append(f"Read exception: {e}")
        
        def write_operation(op_id: int):
            nonlocal write_count
            try:
                value = f"write_value_{op_id}"
                port = HTTP_PORTS[op_id % len(HTTP_PORTS)]
                result = cluster.set_value(port, key, value)
                if result.get("success"):
                    write_count += 1
                else:
                    errors.append(f"Write failed: {result}")
            except Exception as e:
                errors.append(f"Write exception: {e}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for i in range(num_iterations):
                if random.random() < 0.7:
                    futures.append(executor.submit(read_operation, i))
                else:
                    futures.append(executor.submit(write_operation, i))
            concurrent.futures.wait(futures)
        
        assert len(errors) == 0, f"Errors during mixed operations: {errors}"
        assert len(read_values) > 0, "No reads succeeded"
        assert write_count > 0, "No writes succeeded"
        
        for val in read_values:
            assert val is None or isinstance(val, str), f"Invalid read value: {val}"
        
        final_values = []
        for port in HTTP_PORTS:
            result = cluster.get_value(port, key)
            final_values.append(result.get("value"))
        
        assert len(set(final_values)) == 1, f"Final values inconsistent: {final_values}"
        print(f"[PASS] Mixed read/write: {len(read_values)} reads, {write_count} writes")
    
    def test_concurrent_batch_writes(self, cluster: ClusterClient):
        import concurrent.futures
        
        num_batches = 5
        keys_per_batch = 10
        all_keys = {}
        errors = []
        
        def batch_write_operation(batch_id: int):
            try:
                operations = []
                for i in range(keys_per_batch):
                    key = f"concurrent_batch_{batch_id}_key_{i}"
                    value = f"concurrent_batch_{batch_id}_value_{i}"
                    operations.append({"op": "set", "key": key, "value": value})
                    all_keys[key] = value
                
                port = HTTP_PORTS[batch_id % len(HTTP_PORTS)]
                result = cluster.batch_write(port, operations)
                if not result.get("success"):
                    errors.append(f"Batch {batch_id} failed: {result}")
            except Exception as e:
                errors.append(f"Batch {batch_id} exception: {e}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_batches) as executor:
            futures = [executor.submit(batch_write_operation, i) for i in range(num_batches)]
            concurrent.futures.wait(futures)
        
        assert len(errors) == 0, f"Errors during concurrent batch writes: {errors}"
        expected_count = num_batches * keys_per_batch
        assert len(all_keys) == expected_count, f"Expected {expected_count} keys, got {len(all_keys)}"
        
        for port in HTTP_PORTS:
            for key, expected_value in all_keys.items():
                result = cluster.get_value(port, key)
                assert result.get("value") == expected_value, f"Value mismatch for {key} on port {port}"
        
        print(f"[PASS] Concurrent batch writes: {len(all_keys)} keys written")
    
    def test_concurrent_transactions(self, cluster: ClusterClient):
        import concurrent.futures
        
        num_transactions = 10
        results = []
        errors = []
        
        def transaction_operation(txn_id: int):
            try:
                key = f"concurrent_txn_key_{txn_id}"
                value = f"concurrent_txn_value_{txn_id}"
                conditions = [{"key": key, "op": "not_exists"}]
                if_then = [{"op": "set", "key": key, "value": value}]
                else_then = [{"op": "set", "key": key, "value": f"else_{value}"}]
                
                port = HTTP_PORTS[txn_id % len(HTTP_PORTS)]
                result = cluster.txn(port, conditions, if_then, else_then)
                
                if result.get("success"):
                    results.append((key, result.get("branch")))
                else:
                    errors.append(f"Transaction {txn_id} failed: {result}")
            except Exception as e:
                errors.append(f"Transaction {txn_id} exception: {e}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_transactions) as executor:
            futures = [executor.submit(transaction_operation, i) for i in range(num_transactions)]
            concurrent.futures.wait(futures)
        
        assert len(errors) == 0, f"Errors during concurrent transactions: {errors}"
        assert len(results) == num_transactions, f"Expected {num_transactions} results, got {len(results)}"
        
        for key, branch in results:
            for port in HTTP_PORTS:
                result = cluster.get_value(port, key)
                assert result.get("value") is not None, f"Key {key} not found on port {port}"
        
        print(f"[PASS] Concurrent transactions: {len(results)} transactions completed")
    
    def test_concurrent_getset(self, cluster: ClusterClient):
        import concurrent.futures
        import threading
        
        key = "concurrent_getset_key"
        initial_value = "getset_initial"
        num_operations = 15
        
        cluster.set_value(HTTP_PORTS[0], key, initial_value)
        
        old_values = []
        lock = threading.Lock()
        errors = []
        
        def getset_operation(op_id: int):
            try:
                new_value = f"getset_value_{op_id}"
                port = HTTP_PORTS[op_id % len(HTTP_PORTS)]
                result = cluster.getset(port, key, new_value)
                
                if result.get("success"):
                    with lock:
                        old_values.append(result.get("old_value"))
                else:
                    with lock:
                        errors.append(f"Getset {op_id} failed: {result}")
            except Exception as e:
                with lock:
                    errors.append(f"Getset {op_id} exception: {e}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(getset_operation, i) for i in range(num_operations)]
            concurrent.futures.wait(futures)
        
        assert len(errors) == 0, f"Errors during concurrent getset: {errors}"
        assert len(old_values) == num_operations, f"Expected {num_operations} results, got {len(old_values)}"
        
        final_values = []
        for port in HTTP_PORTS:
            result = cluster.get_value(port, key)
            final_values.append(result.get("value"))
        
        assert len(set(final_values)) == 1, f"Final values inconsistent: {final_values}"
        print(f"[PASS] Concurrent getset: {len(old_values)} operations completed")
    
    def test_concurrent_writes_from_all_nodes(self, cluster: ClusterClient):
        import concurrent.futures
        
        keys_per_node = 10
        all_keys = {}
        errors = []
        
        def write_from_node(node_idx: int):
            try:
                port = HTTP_PORTS[node_idx]
                for i in range(keys_per_node):
                    key = f"node{node_idx + 1}_key_{i}"
                    value = f"node{node_idx + 1}_value_{i}"
                    result = cluster.set_value(port, key, value)
                    if result.get("success"):
                        all_keys[key] = value
                    else:
                        errors.append(f"Write from node{node_idx + 1} failed: {result}")
            except Exception as e:
                errors.append(f"Node{node_idx + 1} exception: {e}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(HTTP_PORTS)) as executor:
            futures = [executor.submit(write_from_node, i) for i in range(len(HTTP_PORTS))]
            concurrent.futures.wait(futures)
        
        assert len(errors) == 0, f"Errors during concurrent writes from nodes: {errors}"
        expected_count = len(HTTP_PORTS) * keys_per_node
        assert len(all_keys) == expected_count, f"Expected {expected_count} keys, got {len(all_keys)}"
        
        for port in HTTP_PORTS:
            for key, expected_value in all_keys.items():
                result = cluster.get_value(port, key)
                assert result.get("value") == expected_value, (
                    f"Value mismatch for {key} on port {port}: expected {expected_value}, got {result.get('value')}"
                )
        
        print(f"[PASS] Concurrent writes from all nodes: {len(all_keys)} keys written")
    
    def test_concurrent_high_load(self, cluster: ClusterClient):
        import concurrent.futures
        import random
        
        num_operations = 100
        success_count = 0
        errors = []
        lock = __import__('threading').Lock()
        
        def random_operation(op_id: int):
            nonlocal success_count
            try:
                op_type = random.choice(['set', 'get', 'batch', 'txn'])
                port = HTTP_PORTS[op_id % len(HTTP_PORTS)]
                
                if op_type == 'set':
                    key = f"highload_key_{op_id}"
                    value = f"highload_value_{op_id}"
                    result = cluster.set_value(port, key, value)
                    if result.get("success"):
                        with lock:
                            success_count += 1
                
                elif op_type == 'get':
                    key = f"highload_key_{random.randint(0, 50)}"
                    cluster.get_value(port, key)
                    with lock:
                        success_count += 1
                
                elif op_type == 'batch':
                    operations = [
                        {"op": "set", "key": f"highload_batch_{op_id}_0", "value": f"val_{op_id}"},
                        {"op": "set", "key": f"highload_batch_{op_id}_1", "value": f"val_{op_id}"},
                    ]
                    result = cluster.batch_write(port, operations)
                    if result.get("success"):
                        with lock:
                            success_count += 1
                
                elif op_type == 'txn':
                    key = f"highload_txn_{op_id}"
                    conditions = [{"key": key, "op": "not_exists"}]
                    if_then = [{"op": "set", "key": key, "value": f"txn_val_{op_id}"}]
                    result = cluster.txn(port, conditions, if_then)
                    if result.get("success"):
                        with lock:
                            success_count += 1
                            
            except Exception as e:
                with lock:
                    errors.append(f"Operation {op_id} exception: {e}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(random_operation, i) for i in range(num_operations)]
            concurrent.futures.wait(futures)
        
        success_rate = success_count / num_operations
        assert success_rate >= 0.8, f"Success rate too low: {success_rate:.2%} ({success_count}/{num_operations})"
        
        if errors:
            print(f"[INFO] {len(errors)} errors during high load test (expected some)")
        
        print(f"[PASS] High load test: {success_count}/{num_operations} operations succeeded ({success_rate:.1%})")


def main():
    """Main entry point for direct execution."""
    pytest.main([__file__, "-v"])


if __name__ == "__main__":
    main()
