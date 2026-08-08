# Cross-Chain Fund Protocol & Topic0 Reference Guide

This document serves as the authoritative, human-readable reference for all smart contract addresses, `topic0` event signatures, and byte-decoding rules used across the `v2_pipeline` dbt macros and models.

---

## 1. deBridge DLN

### Protocol Overview
- **Type**: Intent-based cross-chain liquidity network (solver/fulfiller model).
- **Core Identifiers**: `order_id` (32-byte hex string extracted from event data slots).

### Smart Contracts
| Chain | Role | Address |
|---|---|---|
| **Ethereum** | DLN Source | `0xef4fb24ad0916217251f553c0596f8edc630eb66` |
| **Ethereum** | DLN Destination | `0xe7351fd770a37282b91d153ee690b63579d6dd7f` |
| **Arbitrum** | DLN Source | `0xef4fb24ad0916217251f553c0596f8edc630eb66` |
| **Arbitrum** | DLN Destination | `0xe7351fd770a37282b91d153ee690b63579d6dd7f` |
| **Optimism** | DLN Source | `0xef4fb24ad0916217251f553c0596f8edc630eb66` |
| **Optimism** | DLN Destination | `0xe7351fd770a37282b91d153ee690b63579d6dd7f` |
| **Avalanche** | DLN Source | `0xef4fb24ad0916217251f553c0596f8edc630eb66` |
| **Avalanche** | DLN Destination | `0xe7351fd770a37282b91d153ee690b63579d6dd7f` |

### Event Topic0 Hashes
| Event Name | Topic0 Hash | Solidity Event Signature |
|---|---|---|
| **CreatedOrder** | `0xfc8703fd57380f9dd234a89dce51333782d49c5902f307b02f03e014d18fe471` | `CreatedOrder(bytes32 orderId, tuple order, bytes metadata)` |
| **FulfilledOrder** | `0xc164aca37b9805a1c9027b6f32260a069723a82926f6e9ece4926e4dd3ea8ecf` | `FulfilledOrder(bytes32 orderId, address fulfiller, uint256 amountReceived)` |
| **FulfilledOrder (v1)** | `0xe7b447743152a514d14217154942dcfb275ec9c490a6f8090715cf486e589926` | `FulfilledOrderLegacy(bytes32 orderId, address fulfiller)` |

---

## 2. Mayan Swift V2

### Protocol Overview
- **Type**: Cross-chain auction protocol powered by Wormhole messaging & Solana/EVM solvers.
- **Core Identifiers**: `order_key` (32-byte hex string extracted from data slot 0).

### Smart Contracts
| Chain | Role | Address |
|---|---|---|
| **Ethereum** | Mayan Source | `0x40ffe85a28dc9993541449464d7529a922142960` |
| **Ethereum** | Mayan Destination | `0xD78D199f8C402e7B5Cc2abE278dF0412400a3BAe` |
| **Arbitrum** | Mayan Source | `0x40ffe85a28dc9993541449464d7529a922142960` |
| **Arbitrum** | Mayan Destination | `0xD78D199f8C402e7B5Cc2abE278dF0412400a3BAe` |
| **Optimism** | Mayan Source | `0x40ffe85a28dc9993541449464d7529a922142960` |
| **Optimism** | Mayan Destination | `0xD78D199f8C402e7B5Cc2abE278dF0412400a3BAe` |
| **Avalanche** | Mayan Source | `0x40ffe85a28dc9993541449464d7529a922142960` |
| **Avalanche** | Mayan Destination | `0xD78D199f8C402e7B5Cc2abE278dF0412400a3BAe` |

### Event Topic0 Hashes
| Event Name | Topic0 Hash | Solidity Event Signature |
|---|---|---|
| **OrderCreated** | `0x918554b6bd6e2895ce6553de5de0e1a69db5289aa0e4fe193a0dcd1f14347477` | `OrderCreated(bytes32 key, address tokenIn, uint256 amountIn, uint64 destChainId)` |
| **OrderFulfilled** | `0x6ec9b1b5a9f54d929394f18dac4ba1b1cc79823f2266c2d09cab8a3b4700b40b` | `OrderFulfilled(bytes32 key, address tokenOut, uint256 amountOut, address recipient)` |

---

## 3. Stargate V1

### Protocol Overview
- **Type**: Unified liquidity pool bridge powered by LayerZero V1.
- **Core Identifiers**: LayerZero `nonce` + `source_chain_id`.

### Smart Contracts
| Chain | Role | Address |
|---|---|---|
| **Ethereum** | Stargate Router | `0x8731d54e9d02c286767d56ac03e8037c07e01e98` |
| **Arbitrum** | Stargate Router | `0x53Bf83B8b021323808273722aB9a19c6396e95c1` |

### Event Topic0 Hashes
| Event Name | Topic0 Hash | Solidity Event Signature |
|---|---|---|
| **Swap** | `0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f` | `Swap(uint16 chainId, uint256 srcPoolId, uint256 dstPoolId, address from, uint256 amountSD)` |
| **PacketReceived** | `0x2bd2d8a84b748439fd50d79a49502b4eb5faa25b864da6a9ab5c150704be9a4d` | `PacketReceived(uint16 srcChainId, bytes srcAddress, uint64 nonce)` |

---

## 4. Stargate V2

### Protocol Overview
- **Type**: Modular omnichain liquidity bridge powered by LayerZero V2 Endpoint (Bus & Taxi batching).
- **Core Identifiers**: `guid` (32-byte LayerZero V2 message ID).

### Smart Contracts
| Chain | Role | Address |
|---|---|---|
| **Ethereum / Multi-chain** | TokenMessaging Router | `0x19cfce47ed54a88614648dc3f19a5980097007dd` |

### Event Topic0 Hashes
| Event Name | Topic0 Hash | Solidity Event Signature |
|---|---|---|
| **OFTSent (Taxi)** | `0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a` | `OFTSent(bytes32 indexed guid, uint32 dstEid, address indexed fromAddress, uint256 amountSentLD)` |
| **BusDriven (Bus)** | `0x1623f9ea59bd6f214c9571a892da012fc23534aa5906bef4ae8c5d15ee7d2d6e` | `BusDriven(uint32 dstEid, uint72 passId, bytes32 guid)` |
| **BusRode** | `0xe62c9535eb9faefdf05a0b784a0d9b4b025a1e2f8ff5a3b2b4e85785006b528a` | `BusRode(uint32 dstEid, address passenger, uint256 amount)` |
| **OFTReceived** | `0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c` | `OFTReceived(bytes32 indexed guid, uint32 srcEid, address indexed toAddress, uint256 amountReceivedLD)` |

---

## 5. Portal Bridge (Wormhole)

### Protocol Overview
- **Type**: Generalized cross-chain token bridge powered by Wormhole Core Guardians.
- **Core Identifiers**: `sequence` number + `emitter_address`.

### Smart Contracts
| Chain | Role | Address |
|---|---|---|
| **Ethereum** | Wormhole Core | `0xa5f208e072434bc67592e4c49c1b991ba79bca46` |
| **Ethereum** | Portal Token Bridge | `0x0b2402144bb366a632d14b83f244d2e0e21bd39c` |
| **Arbitrum** | Wormhole Core | `0xa5f208e072434bc67592e4c49c1b991ba79bca46` |
| **Arbitrum** | Portal Token Bridge | `0x0b2402144bb366a632d14b83f244d2e0e21bd39c` |

### Event Topic0 Hashes
| Event Name | Topic0 Hash | Solidity Event Signature |
|---|---|---|
| **LogMessagePublished** | `0x6eb224fb001ed210e379b335e35efe88672a8ce935d981a6896b27ffdf52a3b2` | `LogMessagePublished(address sender, uint64 sequence, uint32 nonce, bytes payload, uint8 consistencyLevel)` |
| **TransferRedeemed** | `0xcaf280c8cfeba144da67230d9b009c8f868a75bac9a528fa0474be1ba317c169` | `TransferRedeemed(uint16 emitterChainId, bytes32 emitterAddress, uint64 sequence)` |

---

## Technical Appendix: EVM Data Conventions

1. **ERC-20 Transfer Event Signature**:
   - `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef` -> `Transfer(address indexed from, address indexed to, uint256 value)`
2. **Native ETH Sentinel Address**:
   - `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee` is used when a transaction moves native ETH instead of an ERC-20 token contract.
