#!/usr/bin/env python3
"""Exercise the actual embedded guards with Python optimization enabled, offline."""
import json
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]


def run_fragment(code, args=(), stdin=""):
    return subprocess.run([sys.executable, "-O", "-c", code, *args], input=stdin,
                          text=True, capture_output=True, check=False)


def fragments(name):
    return re.findall(r"<<'PY'\n(.*?)\nPY", (ROOT / ("tests/runners" if name.startswith("test-") else "scripts") / name).read_text(), re.S)


class ScriptPolicyTests(unittest.TestCase):
    def test_tool_versions_remain_enforced_under_optimization(self):
        cases = [("regenerate-protocol-fixtures.sh", "Foundry", "Version: 1.8.4"),
                 ("regenerate-protocol-fixtures.sh", "Solc", "Version: 0.8.31+commit.fd3a2265"),
                 ("test-local-protocol.sh", "Anvil", "Version: 1.8.4")]
        for name, tool, valid in cases:
            with self.subTest(tool=tool):
                candidates = re.findall(r"python3 -c '([^']*)'", (ROOT / ("tests/runners" if name.startswith("test-") else "scripts") / name).read_text())
                code = next(code for code in candidates if tool in code)
                self.assertEqual(run_fragment(code, stdin=valid).returncode, 0)
                invalid = run_fragment(code, stdin="Version: unsupported")
                self.assertNotEqual(invalid.returncode, 0)
                self.assertIn("required", invalid.stderr)

    def test_rpc_chain_guard_remains_enforced_without_network(self):
        code = next(code for code in fragments("test-local-protocol.sh")
                    if "local chain identity mismatch" in code)
        # Replace the transport, not the guard: the script still parses a JSON RPC response.
        for chain, expected in [("0x7a69", 0), ("0x279f", 1)]:
            with self.subTest(chain=chain):
                prefix = ("import io, types, urllib.request\n"
                          f"response = {json.dumps({'result': chain})!r}.encode()\n"
                          "urllib.request.build_opener = lambda *a: types.SimpleNamespace("
                          "open=lambda *a, **k: io.BytesIO(response))\n")
                result = run_fragment(prefix + code, ["http://127.0.0.1:1", "/unused/local.log"])
                self.assertEqual(result.returncode, expected, result.stderr)

    def test_keyless_example_guards_remain_enforced_under_optimization(self):
        code = next(code for code in fragments("test-local-protocol.sh")
                    if "keyless example read the real local pool" in code)
        valid = {"kind": "bootstrapped", "detail": {"identity": {"chain_id": 31337},
                 "as_of": {"number": 0}, "positions": []}}
        cases = [("valid", valid, 0)]
        for field, value in [("kind", "progress"), ("chain_id", 10143),
                             ("number", 1), ("positions", ["unexpected"])]:
            altered = json.loads(json.dumps(valid))
            if field == "kind": altered[field] = value
            elif field == "chain_id": altered["detail"]["identity"][field] = value
            elif field == "number": altered["detail"]["as_of"][field] = value
            else: altered["detail"][field] = value
            cases.append((field, altered, 1))
        with tempfile.TemporaryDirectory(prefix="bl-state-policy-") as temporary:
            path = pathlib.Path(temporary) / "result.json"
            for name, result, expected in cases:
                with self.subTest(field=name):
                    path.write_text(json.dumps(result))
                    checked = run_fragment(code, [str(path)])
                    self.assertEqual(checked.returncode, expected, checked.stderr)


if __name__ == "__main__":
    unittest.main()
