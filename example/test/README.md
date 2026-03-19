# Cluster Tests

This directory contains tests for the cluster example using pytest.

## Requirements

- Python 3.7+
- pytest

Install pytest if not already installed:
```bash
pip install pytest
```

## Test Files

### test_cluster.py

Tests cluster membership and data read/write functionality using pytest framework.

**What it tests:**
1. Builds the project with `cargo build`
2. Starts the cluster using `start.sh`
3. Waits for all nodes to be ready
4. Waits for cluster membership to sync
5. Runs pytest test cases:

**Membership Tests (`TestClusterMembers`):**
   - `test_members_response_structure` - Verifies response format
   - `test_members_ids` - Verifies correct node IDs
   - `test_members_details` - Verifies member details
   - `test_members_consistency_across_nodes` - Verifies all nodes agree

**Data Read/Write Tests (`TestClusterData`):**
   - `test_write_and_read_on_same_node` - Tests basic write/read on same node
   - `test_write_and_read_across_nodes` - Tests write on one node, read from another
   - `test_data_consistency_across_all_nodes` - Tests data consistency across all nodes
   - `test_read_nonexistent_key` - Tests reading a non-existent key
   - `test_overwrite_existing_key` - Tests overwriting an existing key
   - `test_write_special_characters` - Tests special characters and Unicode in values
   - `test_scan_prefix_basic` - Tests scanning keys by prefix
   - `test_scan_prefix_empty_result` - Tests scanning with non-matching prefix
   - `test_scan_prefix_across_nodes` - Tests prefix scan consistency across nodes
   - `test_scan_prefix_nested_prefix` - Tests hierarchical prefix scanning

**Batch Write Tests (`TestBatchWrite`):**
   - `test_batch_write_multiple_sets` - Tests batch writing multiple set operations
   - `test_batch_write_mixed_operations` - Tests batch write with set and delete operations
   - `test_batch_write_empty_operations` - Tests batch write with empty operations list
   - `test_batch_write_single_operation` - Tests batch write with a single operation
   - `test_batch_write_special_characters` - Tests special characters in batch operations
   - `test_batch_write_consistency_across_nodes` - Tests batch write consistency across all nodes
   - `test_batch_write_large_batch` - Tests batch write with 50 operations

6. Stops the cluster

**Usage:**

```bash
# Run all tests with pytest
cd examples/cluster
python3 -m pytest test/test_cluster.py -v

# Or run directly
python3 test/test_cluster.py

# Run specific test
python3 -m pytest test/test_cluster.py::TestClusterMembers::test_members_ids -v
```

## API Endpoints Used

The tests use the following HTTP endpoints:

- `GET /health` - Check if a node is ready
- `GET /members` - Get cluster membership information
- `POST /set` - Set a key-value pair
- `GET /get?key=<key>` - Get a value by key
- `POST /batch_write` - Batch write multiple operations atomically

Example response from `/members`:
```json
{
  "success": true,
  "members": {
    "1": {
      "node_id": 1,
      "endpoint": "127.0.0.1:7001"
    },
    "2": {
      "node_id": 2,
      "endpoint": "127.0.0.1:7002"
    },
    "3": {
      "node_id": 3,
      "endpoint": "127.0.0.1:7003"
    }
  }
}
```

Example request to `/set`:
```bash
curl -X POST http://127.0.0.1:8001/set \
  -H "Content-Type: application/json" \
  -d '{"key": "mykey", "value": "myvalue"}'
```

Example response from `/set`:
```json
{
  "success": true,
  "message": "Key 'mykey' set successfully"
}
```

Example request to `/get`:
```bash
curl -s "http://127.0.0.1:8001/get?key=mykey"
```

Example response from `/get`:
```json
{
  "key": "mykey",
  "value": "myvalue"
}
```

Example request to `/batch_write`:
```bash
curl -X POST http://127.0.0.1:8001/batch_write \
  -H "Content-Type: application/json" \
  -d '{
    "operations": [
      {"op": "set", "key": "key1", "value": "value1"},
      {"op": "set", "key": "key2", "value": "value2"},
      {"op": "delete", "key": "key3"}
    ]
  }'
```

Example response from `/batch_write`:
```json
{
  "success": true,
  "message": "Batch write successful: 3 operations applied"
}
```

## Manual Testing

You can also test the cluster manually using curl:

```bash
# Start the cluster
cd examples/cluster
./start.sh start

# Wait a few seconds for leader election

# Check health of all nodes
for port in 8001 8002 8003; do
    echo "Node $port:"
    curl -s http://127.0.0.1:$port/health | jq .
done

# Get members from all nodes
for port in 8001 8002 8003; do
    echo "Node $port members:"
    curl -s http://127.0.0.1:$port/members | jq .
done

# Stop the cluster
./start.sh stop
```

## Adding New Tests

To add new tests:

1. Add test methods to an existing test class (e.g., `TestClusterMembers` or `TestClusterData`)
   or create a new test class
2. Use the `cluster` fixture which provides a `ClusterClient` instance
3. Follow pytest naming conventions (`test_*`)

Example:
```python
def test_my_new_test(self, cluster: ClusterClient):
    """Test description."""
    for port in HTTP_PORTS:
        result = cluster.query_members(port)
        # Add your assertions here
        assert len(result["members"]) > 0
```

### Available ClusterClient Methods

- `query_members(port)` - Query cluster membership from a node
- `set_value(port, key, value)` - Set a key-value pair via a node
- `get_value(port, key)` - Get a value by key via a node
- `query_prefix(port, prefix)` - Scan keys by prefix via a node
- `batch_write(port, operations)` - Execute batch write operations atomically

## Troubleshooting

### Test fails with "Node failed to start"

- Check if ports 8001, 8002, 8003 are already in use
- Check if Raft ports 7001, 7002, 7003 are already in use
- Run `./start.sh clean-all` to clean up stale data
- Check the logs: `./start.sh logs node1`

### pytest not found

```bash
pip install pytest
# or
pip3 install pytest
```
