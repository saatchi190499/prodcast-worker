petex_client/
├── __init__.py
├── exceptions.py
├── server.py          # Safe COM wrapper + core primitives (get/set/cmd)
├── utils.py           # helpers (parsing, list<->GAP strings, masking)
├── gap.py             # GAP-specific convenience methods
├── resolve.py         # RESOLVE-specific convenience methods
└── compat.py          # Backward-compatible shims (your old function names)