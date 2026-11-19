# Security policy

## Report a vulnerability

Please do not report security vulnerabilities in public GitHub issues.

Report them privately through GitHub instead:

1. Open the [Security tab](https://github.com/linkedfactory/linkedfactory-pod/security) of this repository.
2. Select **Report a vulnerability**, or go directly to [the reporting form](https://github.com/linkedfactory/linkedfactory-pod/security/advisories/new).

Only the maintainers can see the report. They are notified by GitHub and will get back to you in the report.

## What to include

- The affected version or commit of LinkedFactory Pod
- The affected component or endpoint, for example `/linkedfactory/values` or `/sparql`
- Steps to reproduce, and the configuration used if it differs from the supplied defaults
- The impact you expect, for example data disclosure or unauthorised writes

## Development defaults

The supplied configuration and Docker image are meant for local development. They contain the default account `admin` / `admin` and grant anonymous access to models. Reports that only concern these documented defaults are not vulnerabilities; see [Secure a deployment](docs/modules/how-to/pages/secure-deployment.adoc) for hardening a deployment.
