#!/usr/bin/env python3
"""
Chaos engineering tests for rockraft cluster on Kubernetes with Chaos Mesh.

Prerequisites:
  - kind cluster with Chaos Mesh installed
  - rockraft StatefulSet deployed (3 pods)
  - pip install pyyaml

Run:
  python3 -m pytest test/test_chaos.py -v
"""

import json
import subprocess
import time
import urllib.parse
from pathlib import Path

import pytest
import yaml

NAMESPACE = "rockraft"
POD_NAMES = ["rockraft-0", "rockraft-1", "rockraft-2"]
CHAOS_DIR = Path(__file__).parent.parent / "chaos" / "crds"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def kubectl(*args, input_data=None, check=True):
    cmd = ["kubectl"] + list(args)
    result = subprocess.run(
        cmd, capture_output=True, text=True, input=input_data,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed:\n{result.stderr}")
    return result


def http_get(ordinal, path, timeout=5):
    """Execute curl inside the pod via kubectl exec."""
    pod = POD_NAMES[ordinal]
    try:
        result = subprocess.run(
            [
                "kubectl", "exec", "-n", NAMESPACE, pod, "--",
                "curl", "-sf", "--max-time", str(timeout),
                f"http://localhost:8000{path}",
            ],
            capture_output=True, text=True, timeout=timeout + 10,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        print(f"http_get({pod}, {path}) failed: rc={result.returncode} stderr={result.stderr.strip()}")
    except subprocess.TimeoutExpired:
        print(f"http_get({pod}, {path}) timed out after {timeout+10}s")
    except Exception as e:
        print(f"http_get({pod}, {path}) exception: {e}")
    return None


def http_post(ordinal, path, data, timeout=10):
    """Execute curl inside the pod via kubectl exec."""
    pod = POD_NAMES[ordinal]
    body = json.dumps(data)
    try:
        result = subprocess.run(
            [
                "kubectl", "exec", "-n", NAMESPACE, pod, "--",
                "curl", "-sf", "--max-time", str(timeout),
                "-X", "POST",
                "-H", "Content-Type: application/json",
                "-d", body,
                f"http://localhost:8000{path}",
            ],
            capture_output=True, text=True, timeout=timeout + 10,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        try:
            return json.loads(result.stderr)
        except Exception:
            return {"error": result.stderr}
    except Exception as e:
        return {"error": str(e)}


def wait_for(condition_fn, timeout=60, interval=1, message="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = condition_fn()
        if result:
            return result
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for {message} ({timeout}s)")


# ---------------------------------------------------------------------------
# Cluster client
# ---------------------------------------------------------------------------

class ClusterClient:
    def get_health(self, ordinal):
        return http_get(ordinal, "/health")

    def get_leader_ordinal(self):
        for i in range(3):
            h = self.get_health(i)
            if h and h.get("is_leader"):
                return i
        return None

    def set_value(self, ordinal, key, value):
        return http_post(ordinal, "/set", {"key": key, "value": value})

    def get_value(self, ordinal, key):
        return http_get(ordinal, f"/get?key={urllib.parse.quote(key)}")

    def write_to_leader(self, key, value):
        leader = self.get_leader_ordinal()
        assert leader is not None, "No leader available"
        return self.set_value(leader, key, value), leader

    def verify_value_on_all_nodes(self, key, expected):
        for i in range(3):
            h = self.get_health(i)
            if not h:
                continue
            result = self.get_value(i, key)
            actual = result.get("value") if result else None
            assert actual == expected, (
                f"Node {i} value mismatch: expected={expected}, got={actual}"
            )

    def wait_for_leader(self, timeout=30):
        def _check():
            for i in range(3):
                h = self.get_health(i)
                leader = h.get("is_leader") if h else None
                print(f"  node {i} (pod {POD_NAMES[i]}): is_leader={leader}, raw={h}")
                if leader:
                    return i
            return None
        return wait_for(_check, timeout=timeout, message="leader election")

    def wait_for_stable_leader(self, timeout=20, stable_for=5):
        same_since = time.time()
        last_leader = None
        deadline = time.time() + timeout
        while time.time() < deadline:
            leader = self.get_leader_ordinal()
            if leader is None:
                same_since = time.time()
                last_leader = None
            elif leader != last_leader:
                same_since = time.time()
                last_leader = leader
            elif time.time() - same_since >= stable_for:
                return leader
            time.sleep(1)
        raise TimeoutError(f"Leader not stable for {stable_for}s within {timeout}s")


# ---------------------------------------------------------------------------
# Chaos Mesh helper
# ---------------------------------------------------------------------------

class ChaosHelper:
    def apply_yaml(self, yaml_str):
        kubectl("apply", "-f", "-", input_data=yaml_str)

    def delete(self, kind, name):
        kubectl("delete", kind, name, "-n", NAMESPACE, "--ignore-not-found", check=False)

    def get_status(self, kind, name, jsonpath):
        result = kubectl(
            "get", kind, name, "-n", NAMESPACE,
            "-o", f"jsonpath={{{jsonpath}}}",
            check=False,
        )
        return result.stdout.strip()

    def wait_running(self, kind, name, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            phase = self.get_status(kind, name, ".status.experiment.desiredPhase")
            if phase == "Run":
                return True
            time.sleep(1)
        return False

    def cleanup_all(self):
        for kind in ["PodChaos", "NetworkChaos", "IOChaos", "StressChaos", "TimeChaos"]:
            kubectl("delete", kind, "--all", "-n", NAMESPACE, check=False)
        time.sleep(2)

    def make_pod_kill(self, name, pod_name):
        return yaml.dump({
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "PodChaos",
            "metadata": {"name": name, "namespace": NAMESPACE},
            "spec": {
                "action": "pod-kill",
                "mode": "one",
                "duration": "",
                "selector": {
                    "pods": {NAMESPACE: [pod_name]},
                },
            },
        })

    def make_network_partition(self, name, source_pod, target_pods, duration="30s"):
        return yaml.dump({
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "NetworkChaos",
            "metadata": {"name": name, "namespace": NAMESPACE},
            "spec": {
                "action": "partition",
                "direction": "both",
                "mode": "all",
                "duration": duration,
                "selector": {
                    "pods": {NAMESPACE: [source_pod]},
                },
                "target": {
                    "mode": "all",
                    "selector": {
                        "pods": {NAMESPACE: target_pods},
                    },
                },
            },
        })

    def make_network_delay(self, name, latency="200ms", jitter="50ms", duration="30s"):
        return yaml.dump({
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "NetworkChaos",
            "metadata": {"name": name, "namespace": NAMESPACE},
            "spec": {
                "action": "delay",
                "mode": "all",
                "duration": duration,
                "delay": {
                    "latency": latency,
                    "correlation": "0",
                    "jitter": jitter,
                },
                "selector": {
                    "labelSelectors": {"app": "rockraft"},
                },
            },
        })

    def make_network_loss(self, name, loss_percent="30", duration="30s"):
        return yaml.dump({
            "apiVersion": "chaos-mesh.org/v1alpha1",
            "kind": "NetworkChaos",
            "metadata": {"name": name, "namespace": NAMESPACE},
            "spec": {
                "action": "loss",
                "mode": "all",
                "duration": duration,
                "loss": {
                    "loss": loss_percent,
                    "correlation": "50",
                },
                "selector": {
                    "labelSelectors": {"app": "rockraft"},
                },
            },
        })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def cluster():
    client = ClusterClient()
    client.wait_for_leader(timeout=120)
    time.sleep(3)
    return client


@pytest.fixture
def chaos():
    helper = ChaosHelper()
    yield helper
    helper.cleanup_all()


# ---------------------------------------------------------------------------
# Test: Leader failover via PodChaos (pod-kill)
# ---------------------------------------------------------------------------

class TestLeaderFailover:
    def test_leader_kill_elects_new_leader(self, cluster, chaos):
        leader_before = cluster.wait_for_stable_leader()
        pod_name = POD_NAMES[leader_before]

        key = f"chaos-failover-{int(time.time())}"
        resp, _ = cluster.write_to_leader(key, "before-kill")
        assert resp.get("success"), f"Write failed: {resp}"

        yaml_str = chaos.make_pod_kill("kill-leader-test", pod_name)
        chaos.apply_yaml(yaml_str)
        time.sleep(3)

        leader_after = cluster.wait_for_leader(timeout=30)
        assert leader_after != leader_before, (
            f"Leader should have changed after killing {pod_name}"
        )

        resp = cluster.set_value(leader_after, key, "after-recovery")
        assert resp.get("success"), f"Write to new leader failed: {resp}"

        time.sleep(2)
        cluster.verify_value_on_all_nodes(key, "after-recovery")

    def test_leader_recovers_and_rejoins(self, cluster, chaos):
        leader_before = cluster.wait_for_stable_leader()
        pod_name = POD_NAMES[leader_before]

        key = f"chaos-rejoin-{int(time.time())}"
        resp, _ = cluster.write_to_leader(key, "before")
        assert resp.get("success"), f"Write failed: {resp}"
        time.sleep(2)

        yaml_str = chaos.make_pod_kill("kill-rejoin-test", pod_name)
        chaos.apply_yaml(yaml_str)
        time.sleep(3)

        cluster.wait_for_leader(timeout=30)
        resp, new_leader = cluster.write_to_leader(key, "after")
        assert resp.get("success"), f"Write failed: {resp}"
        time.sleep(3)

        for _ in range(30):
            health = cluster.get_health(leader_before)
            if health and health.get("state") in ("Follower", "Leader", "Candidate"):
                break
            time.sleep(2)
        else:
            pytest.fail(f"Pod {pod_name} did not recover within 60s")

        time.sleep(3)
        result = cluster.get_value(leader_before, key)
        assert result.get("value") == "after", (
            f"Recovered node should have latest data: {result}"
        )


# ---------------------------------------------------------------------------
# Test: Follower kill via PodChaos
# ---------------------------------------------------------------------------

class TestFollowerFailure:
    def test_follower_kill_cluster_still_writes(self, cluster, chaos):
        leader = cluster.wait_for_stable_leader()
        follower_ordinals = [i for i in range(3) if i != leader]
        victim = follower_ordinals[0]
        pod_name = POD_NAMES[victim]

        key = f"chaos-follower-{int(time.time())}"
        resp, _ = cluster.write_to_leader(key, "during-kill")
        assert resp.get("success"), f"Write failed: {resp}"

        yaml_str = chaos.make_pod_kill("kill-follower-test", pod_name)
        chaos.apply_yaml(yaml_str)
        time.sleep(2)

        key2 = f"chaos-follower2-{int(time.time())}"
        resp, _ = cluster.write_to_leader(key2, "still-writable")
        assert resp.get("success"), f"Write should succeed with quorum: {resp}"

        time.sleep(2)
        remaining = [i for i in range(3) if i != victim]
        for i in remaining:
            result = cluster.get_value(i, key2)
            assert result.get("value") == "still-writable", (
                f"Node {i} should have the data: {result}"
            )


# ---------------------------------------------------------------------------
# Test: Network partition (split brain)
# ---------------------------------------------------------------------------

class TestNetworkPartition:
    def test_minority_cannot_write(self, cluster, chaos):
        leader = cluster.wait_for_stable_leader()
        others = [i for i in range(3) if i != leader]

        key = f"chaos-partition-{int(time.time())}"
        resp, _ = cluster.write_to_leader(key, "before-partition")
        assert resp.get("success"), f"Write failed: {resp}"

        leader_pod = POD_NAMES[leader]
        other_pods = [POD_NAMES[i] for i in others]
        yaml_str = chaos.make_network_partition(
            "partition-test", leader_pod, other_pods, duration="60s",
        )
        chaos.apply_yaml(yaml_str)
        time.sleep(5)

        isolated_resp = cluster.set_value(leader, "isolated-key", "from-minority")
        assert not isolated_resp.get("success") or isolated_resp.get("error"), (
            f"Isolated leader (minority) should not be able to write: {isolated_resp}"
        )

        for i in others:
            health = cluster.get_health(i)
            if health and health.get("is_leader"):
                key2 = f"chaos-majority-{int(time.time())}"
                resp = cluster.set_value(i, key2, "from-majority")
                assert resp.get("success"), (
                    f"Majority leader should be writable: {resp}"
                )
                break

        chaos.delete("NetworkChaos", "partition-test")
        time.sleep(5)

        recovered_leader = cluster.wait_for_leader(timeout=30)
        key3 = f"chaos-recovered-{int(time.time())}"
        resp = cluster.set_value(recovered_leader, key3, "after-recovery")
        assert resp.get("success"), f"Write after recovery failed: {resp}"


# ---------------------------------------------------------------------------
# Test: Network delay
# ---------------------------------------------------------------------------

class TestNetworkDelay:
    def test_cluster_works_under_delay(self, cluster, chaos):
        key = f"chaos-delay-before-{int(time.time())}"
        resp, _ = cluster.write_to_leader(key, "before")
        assert resp.get("success")

        yaml_str = chaos.make_network_delay(
            "delay-test", latency="200ms", jitter="50ms", duration="60s",
        )
        chaos.apply_yaml(yaml_str)
        time.sleep(3)

        key2 = f"chaos-delay-during-{int(time.time())}"
        leader = cluster.wait_for_leader(timeout=15)
        resp = cluster.set_value(leader, key2, "during-delay")
        assert resp.get("success"), f"Write should succeed under delay: {resp}"

        time.sleep(3)
        cluster.verify_value_on_all_nodes(key2, "during-delay")

        chaos.delete("NetworkChaos", "delay-test")
        time.sleep(3)

        key3 = f"chaos-delay-after-{int(time.time())}"
        resp, _ = cluster.write_to_leader(key3, "after")
        assert resp.get("success")


# ---------------------------------------------------------------------------
# Test: Packet loss
# ---------------------------------------------------------------------------

class TestNetworkLoss:
    def test_eventual_consistency_under_loss(self, cluster, chaos):
        key = f"chaos-loss-before-{int(time.time())}"
        resp, _ = cluster.write_to_leader(key, "before")
        assert resp.get("success")

        yaml_str = chaos.make_network_loss("loss-test", loss_percent="30", duration="60s")
        chaos.apply_yaml(yaml_str)
        time.sleep(3)

        writes_ok = 0
        writes_total = 5
        for attempt in range(writes_total):
            key2 = f"chaos-loss-{int(time.time())}-{attempt}"
            leader = cluster.get_leader_ordinal()
            if leader is not None:
                resp = cluster.set_value(leader, key2, f"attempt-{attempt}")
                if resp.get("success"):
                    writes_ok += 1
            time.sleep(1)

        assert writes_ok >= 2, (
            f"At least 2 of {writes_total} writes should succeed under 30% loss"
        )

        chaos.delete("NetworkChaos", "loss-test")
        time.sleep(5)

        key3 = f"chaos-loss-after-{int(time.time())}"
        resp, _ = cluster.write_to_leader(key3, "after")
        assert resp.get("success")
        time.sleep(2)
        cluster.verify_value_on_all_nodes(key3, "after")
