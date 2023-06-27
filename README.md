# GDPR

This plugin provides policy hooks to facilitate compliance with GDPR (European regulations for personal data)

See https://www.dataiku.com/product/plugins/gdpr/ to learn more

It helps organizations implement the GDPR defining rules and processes directly in Dataiku.

It provides an example of how organizations can use custom fields and policies to comply with internal policies and external regulations (such as GDPR) around data privacy and protection.

This plugin offers the ability to:

* Document data sources with sensitive information, and enforce good practices
* Restrict access to projects and data sources with sensitive information
* Audit the sensitive information in a Dataiku DSS instance

## Building

This Java plugin cannot be used directly from the DSS development plugin editor. It must be built separately, with a Java stack.

Building this plugin requires a good familiarity with Java code and compilation chains. Ant must be installed.

* Export the DKUINSTALLDIR variable to a dezipped Dataiku kit
* Run "make"
* The plugin zip is available in "dist"

## License

Copyright (c) Dataiku SAS 2019-2023

This plugin is distributed under the [Apache License version 2.0](LICENSE).
