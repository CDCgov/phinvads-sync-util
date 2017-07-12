# Repository Template
This Github organization was created for use by [CDC](http://www.cdc.gov) programs to collaborate on public health surveillance related projects in support of the [CDC Surveillance Strategy](http://www.cdc.gov/surveillance). This third party web application is not hosted by the CDC, but is used by CDC and its partners to share information and collaborate on software.

This repository serves as a template for other repositories to follow in order to provide the appropriate notices for users in regards to privacy protection, contribution, licensing, copyright, records management and collaboration.


## Public Domain:
This project constitutes a work of the United States Government and is not subject to domestic copyright protection under 17 USC § 105. This project is in the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/). All contributions
to this project will be released under the CC0 dedication. By submitting a pull request you are agreeing
to comply with this waiver of copyright interest.

## License
The project utilizes code licensed under the terms of the Apache Software License and therefore is licensed under ASL v2 or later.

This program is free software: you can redistribute it and/or modify it under the terms of the Apache Software License version 2, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

## Privacy
This project contains only non-sensitive, publicly available data and information. All material and community participation is covered by the Surveillance Platform [Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md) and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md). For more information about CDC's privacy policy, please visit [http://www.cdc.gov/privacy.html](http://www.cdc.gov/privacy.html).

## Contributing
Anyone is encouraged to contribute to the project by [forking](https://help.github.com/articles/fork-a-repo) and submitting a pull request. (If you are new to GitHub, you might start with a [basic tutorial](https://help.github.com/articles/set-up-git).)
By contributing to this project, you grant a world-wide, royalty-free, perpetual, irrevocable, non-exclusive, transferable license to all users under the terms of the [Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or later.

All comments, messages, pull requests, and other submissions received through CDC including this GitHub page are subject to the [Presidential Records Act](http://www.archives.gov/about/laws/presidential-records.html) and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records
This project is not a source of government records, but is a copy to increase collaboration and collaborative potential. All government records will be published through the [CDC web site](http://www.cdc.gov.)

## Notices
Please refer to [CDC's Template Repository](https://github.com/CDCgov/template) for more information about [contributing to this repository](https://github.com/CDCgov/template/blob/master/CONTRIBUTING.md), [public domain notices and disclaimers](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md), and [code of conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).

## Hat-tips
Thanks to [18F](https://18f.gsa.gov/)'s [open source policy](https://github.com/18F/open-source-policy) and [code of conduct](https://github.com/CDCgov/code-of-conduct/blob/master/code-of-conduct.md) that were very useful in setting up this GitHub organization. Thanks to CDC's [Informatics Innovation Unit](https://www.phiresearchlab.org/index.php/code-of-conduct/) that was helpful in modeling the code of conduct.

## Sync Utility
The software contained in this repository is used to sync the code systems and valuesets contained within PHIN VADS into an elasticsearch server.

### Code Systems
Code systems are mapped into elastic search in 2 different indexes.

Code system meta data is contained in an index with the follwoing semantics.

/code_systems/code_system/{oid}

The codes for a given code system are mapped into elastic search in a separate index that allows for easy searching across code systems as well as scoping to individual code systems.  The index is sturctured as follows.

Codes for individual code systems are added to the codes index scoped under the oid for the coded system that they come from.  The codes are added individually and are not contained in any grouping structure.  The {id} portion of the url described below is generated by Elasticsearch when a code is added to the index and is meaningless outside the context of elastic search

/codes/{oid}/{id}

### Valuesets

Valuesets are synced into the elasticserach server as valueset versions.  Within Phinvads the codes for a given valueset are mapped to versions of a given valueset.  Keeping with that system the valuesets are mapped into elastic search as versions with the given url semantics

 /valueset_versions/{oid}/{version_number}

Each of the versions is formated as a FHIR ValueSet object complete with the codes that they contain.


## Building
```
mvn package
```


## Running
java -jar target/phinvads-sync-util.jar

```
[options]
    -e, --elasticsearch  elastic search host
    -v, --vads           PHINVADS api url
    -o, --operation      operation to run (sync_all, sync_vs[:oid:version], sync_cs[:oid])
    -f, --force          force reindex
```

## Deploying on an Openshift cluster
* Create a service through the Add To Project interface in the Openshift web console
* Choose the redhat-openjdk18-openshift s2i image as the base image (`jdk` in the search bar is the fastest way to find it)
* Name the service and add the github repository url (https://github.com/CDCgov/phinvads-sync-util)
* If necessary, click the advanced options link and add the git reference 
* Disable route creation
* Hit Create
* Wait for maven to run and build the image
* Open the deployment environment config and add:
    * `JAVA_ARGS` as `-e http://${your_elasticsearch_host} -v https://phinvads.cdc.gov/vocabService/v2”`
    * `no_proxy` if necessary to reach your elasticsearch host or phinvads service
* Saving should trigger a redeploy, and the sync service should be running