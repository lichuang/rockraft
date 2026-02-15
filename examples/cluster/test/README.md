# Cluster Tests

This directory contains tests for the cluster example using pytest.

## Requirements

- Python 3.7+
- pytest

Install pytest if not already installed:
```bash
pip install pytest
```

## Test File

### test_cluster_members.py

Tests cluster membership functionality using pytest framework.

**What it tests:**
1. Builds the project with `cargo build`
2. Starts the cluster using `start.sh`
3. Waits for all nodes to be ready
4. Waits for cluster membership to sync
5. Runs pytest test cases:
   - `test_members_response_structure` - Verifies response format
   - `test_members_ids` - Verifies correct node IDs
   - `test_members_details` - Verifies member details
   - `test_members_consistency_across_nodes` - Verifies all nodes agree
6. Stops the cluster

**Usage:**

```bash
# Run all tests with pytest
cd examples/cluster
python3 -m pytest test/test_cluster_members.py -v

# Or run directly
python3 test/test_cluster_members.py

# Run specific test
python3 -m pytest test/test_cluster_members.py::TestClusterMembers::test_members_ids -v
```

## API Endpoints Used

The tests use the following HTTP endpoints:

- `GET /health` - Check if a node is ready
- `GET /members` - Get cluster membership information

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

1. Add test methods to the `TestClusterMembers` class
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
