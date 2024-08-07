# ids.server: Server part of ICAT Data Server (IDS)

## Linting
The [spotless-maven-plugin](https://mvnrepository.com/artifact/com.diffplug.spotless/spotless-maven-plugin) is used here for linting. It uses the Eclipse code formatting feature. The guidelines are defined in `eclipse-formatter.xml` which was initially exported from eclipse (so you can do it again: define rules in eclipse an export the file to use it here).

An automatically code formatting is triggered while local build. In CI builds the code is only validated.
You also can execute the commands manually:
* `mvn spotless:check` for validation
* `mvn spotless:apply` to update the source code like it is defined in the formatters configuration.

### Import order
The import order is also validated and applied by the spotless-maven-plugin. It is defined in the file `eclipse-importorder.txt` and referenced in `pom.xml`. It is needed that it has a compatible format like the eclipse built-in import order configuration (better its exported file).

## Further Information
[![Build Status](https://github.com/icatproject/ids.server/workflows/CI%20Build/badge.svg?branch=master)](https://github.com/icatproject/ids.server/actions?query=workflow%3A%22CI+Build%22)

General installation instructions are at http://www.icatproject.org/installation/component

Specific installation instructions are
at https://repo.icatproject.org/site/ids/server/${project.version}/installation.html

All documentation on ids.server may be found at https://repo.icatproject.org/site/ids/server/${project.version}
