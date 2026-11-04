# Contract ABIs

Rust bindings are in [src/abi.rs](../src/abi.rs).

| File | Contents |
| --- | --- |
| `CreditPool.v5.json` | Complete pool ABI, including getters and orderbooks. |
| `ICreditPoolEvents.v5.json` | Pool events used for discovery. |
| `ICreditPool.v5.json` | Core pool interface; does not include every getter or event. |

The remaining ABIs cover oracle and factory interfaces. Use `HttpReader::contract_call()` with an explicit block for on-demand reads.

[PROVENANCE.json](PROVENANCE.json) records the protocol revision, compiler settings and ABI hashes. [Protocol fixtures](../tests/fixtures/protocol-v5/README.md) cover emitted events and risk calculations.
