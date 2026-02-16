# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

### Added
- CI workflow for linting, type checking, tests, and distribution validation.
- Security and contributing documentation.
- Pluggable cache backend contract (`CacheBackend`) with `DataCache` as default implementation.
- Dataset usage-note metadata for every dataset via explicit or category-derived defaults.
- Five ZINC tranche-based drug-like dataset targets across purchasability tiers (in-stock, agent, wait-ok, boutique, annotated), each configured as multi-tranche fetch targets.

### Changed
- Source validation now treats mirrored URLs as healthy when at least one source is reachable.
- Added dataset URL mode support for concatenating multiple source URLs into one cached raw file (`url_mode="concat"`).
- Refresh fetch now fails instead of silently falling back to stale cached data.
- Development metadata upgraded from Alpha to Beta.
- Fetch and materialize metadata now include normalized dataset snapshots (description, usage notes, source, licensing).
- CLI JSON output for `list` and `fetch` now includes dataset metadata for easier automation.

### Fixed
- mypy issues in catalog and IO typing.
- Known failing fallback dataset URLs in the default catalog.
- Local artifact hygiene via `.gitignore`.
