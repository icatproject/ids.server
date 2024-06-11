# ids.server: Server part of ICAT Data Server (IDS)

## Linting
The [formatter-maven-plugin](https://code.revelc.net/formatter-maven-plugin/) is used here for linting. It uses the same configuration like the Eclipse code formatting feature.
An automatically code formatting is triggered while local build. In CI builds the code is only validated.
You also can execute the commands manually:
* `mvn formatter:validate` for validation
* `mvn formatter:format` to update the source code like it is defined in the formatters configuration.

The rules for the formatter are defined in `eclipse-formatter-config.xml`. 

## Further Information
[![Build Status](https://github.com/icatproject/ids.server/workflows/CI%20Build/badge.svg?branch=master)](https://github.com/icatproject/ids.server/actions?query=workflow%3A%22CI+Build%22)

General installation instructions are at http://www.icatproject.org/installation/component

Specific installation instructions are
at https://repo.icatproject.org/site/ids/server/${project.version}/installation.html

All documentation on ids.server may be found at https://repo.icatproject.org/site/ids/server/${project.version}
