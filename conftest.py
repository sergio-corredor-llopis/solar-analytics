"""Repo-root conftest.

No fixtures live here — this file's only job is to anchor pytest's rootdir
so `from src.<module> import ...` resolves in tests/unit/ without needing a
src/__init__.py or an editable package install (src/ is treated as an
implicit namespace package, which is fine for a script-collection repo like
this one). See tests/unit/ for the actual test suite.
"""
