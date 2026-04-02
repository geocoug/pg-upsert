# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in pg-upsert, please report it responsibly:

1. **Do not open a public issue.**
1. Email [grantcaleb22@gmail.com](mailto:grantcaleb22@gmail.com) with details of the vulnerability.
1. Include steps to reproduce, affected versions, and any potential impact.

I will respond to security reports ASAP. Security fixes will be released as patch versions.

## Trust Model

pg-upsert connects to PostgreSQL databases using credentials provided by the user. It executes SQL operations (SELECT, INSERT, UPDATE) against the specified schemas. There is no sandboxing or privilege separation beyond what PostgreSQL provides.

**Do not use pg-upsert with untrusted configuration files or in environments where database credentials may be exposed.**

## Supported Versions

Security fixes are applied to the latest release only. There is no backport policy for older versions.

| Version  | Supported |
| -------- | --------- |
| latest   | Yes       |
| < latest | No        |
