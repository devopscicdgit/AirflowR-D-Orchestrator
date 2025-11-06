"""Smoke tests ensuring package import structure works."""

def test_import_dags_utils():
    # Should import without ModuleNotFoundError now that dags is a package
    import dags.utils.data_helpers  # noqa: F401
