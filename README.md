# ids.server: Server part of ICAT Data Server (IDS)

## Linting
The Prettier Maven Plugin is used for linting. it is configured for "check" while build. That's why a build will fail when the files haven't the right format.
execute `mvn prettier:write` before.

TODO: execute `mvn prettier:wite` while local build but only the `check` while CI build.

[![Build Status](https://github.com/icatproject/ids.server/workflows/CI%20Build/badge.svg?branch=master)](https://github.com/icatproject/ids.server/actions?query=workflow%3A%22CI+Build%22)

General installation instructions are at http://www.icatproject.org/installation/component

Specific installation instructions are
at https://repo.icatproject.org/site/ids/server/${project.version}/installation.html

All documentation on ids.server may be found at https://repo.icatproject.org/site/ids/server/${project.version}
