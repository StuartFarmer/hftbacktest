from __future__ import annotations

from pathlib import Path

_IMPL_ROOT = Path(__file__).resolve().parent / "py-hftbacktest" / "hftbacktest"
_IMPL_INIT = _IMPL_ROOT / "__init__.py"

__path__ = [str(_IMPL_ROOT)]
__file__ = str(_IMPL_INIT)

exec(compile(_IMPL_INIT.read_text(), str(_IMPL_INIT), "exec"), globals())
