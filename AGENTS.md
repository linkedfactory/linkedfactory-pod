# Instructions for coding agents

LinkedFactory Pod is an Eclipse Equinox/OSGi application on top of eniLINK, written in Java and Scala and built with Maven and bnd.
The repository layout is described in [docs/modules/development/pages/repository-structure.adoc](docs/modules/development/pages/repository-structure.adoc).

## Build and test

- Build: `mvn package`
- Build and test: `mvn verify` (the same command runs in CI)
- Server distribution: `mvn package -Pserver`
- Documentation: `npx antora@3.1 --fetch antora-playbook.yml` (fails on broken cross-references)

## Branches and commits

If the work belongs to a GitHub issue, include its number in the branch name (`gh-123-short-description`) and start each commit message with it (`GH-123 Short description`). Add or update tests with every code change.

## Documentation

All documentation follows the Diátaxis framework. The binding rules are in [docs/modules/development/pages/documentation.adoc](docs/modules/development/pages/documentation.adoc). Read that page before adding or changing documentation.

The rules you must not break:

1. **One type per page.** Every page in `docs/` is exactly one of tutorial, how-to guide, reference, or explanation, and lives in the module of that type (`tutorials`, `how-to`, `reference`, `explanation`). Contributor pages live in `development` and also have one type each.
2. **Classify before writing.** Decide the type from the reader's question: "How do I …?" is a how-to guide, "What exactly is …?" is reference, "Why …?" is explanation, "Teach me" is a tutorial. Content of another type goes to a page of that type, linked from the current page.
3. **Separate the audiences.** Content for people using Pod goes to `README.md` and the user modules. Content for people changing Pod goes to `CONTRIBUTING.md` and the `development` module. `README.md` and `CONTRIBUTING.md` stay short and link into `docs/` instead of repeating it.
4. **Verify against the code.** Check every statement about behaviour, defaults, parameters, and paths against the source and configuration files in this repository.
5. **No placeholders.** Do not add empty pages, outlines, or planning documents to the repository. Add a page only with its content.
6. **Keep the navigation consistent.** Add new pages to the `nav.adoc` of their module. Cross-references name the module: `xref:how-to:query-data.adoc[]`.
7. **Document with the change.** When code changes behaviour, interfaces, or configuration, update the documentation in the same change.

Before finishing a documentation change, run the documentation build and fix all warnings.
