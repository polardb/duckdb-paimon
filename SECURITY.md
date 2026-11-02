# Security Policy

## Supported Versions

Security fixes are prioritized for the maintained branches listed in the [README compatibility matrix](README.md#compatibility). Use the latest available extension release for your DuckDB version. Older releases may require an upgrade to receive a fix.

## Reporting a Vulnerability

Report suspected security vulnerabilities privately through [GitHub's vulnerability reporting form](https://github.com/polardb/duckdb-paimon/security/advisories/new).

Please coordinate with maintainers before sharing vulnerability details in public issues, pull requests, or community chats.

Include the following information when available:

- DuckDB and extension versions, or the commit if built from source.
- Operating system, CPU architecture, and relevant catalog or storage configuration.
- A description of the vulnerability, its potential impact, and any conditions needed to exploit it.
- Reproduction steps, a minimal SQL example, and any required table setup or sample data.
- Relevant error messages, logs, or a proof of concept.

Remove credentials and sensitive data from examples and logs before submitting them.

## Handling Reports

Maintainers assess reports based on severity and impact, discuss findings and remediation through the private advisory, and coordinate disclosure with the reporter. Response and remediation timing depend on the issue and maintainer availability.
