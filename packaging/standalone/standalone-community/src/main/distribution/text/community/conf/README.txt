${product.fullname} Configuration
=======================================

${product.fullname} configuration files.

* custom-logback.xml                 -- logging system configuration for the database
                                        process using standard logback options
* neo4j-http-logging.xml             -- logging system configuration for the HTTP REST server
                                        using standard logback options
* neo4j-server.properties            -- runtime operational settings
* neo4j.properties                   -- database tuning parameters
* neo4j-wrapper.conf                 -- environment and launch settings for Neo4j Server
* loggging.properties                -- java.util.logging settings for Neo4j Server
* windows-wrapper-logging.properties -- java.util.logging settings for the windows service wrapper
* ssl/                               -- directory holding certificate files for
                                        HTTPS operation of the REST server


References
----------

* [Java Logging Overview](http://docs.oracle.com/javase/7/docs/technotes/guides/logging/overview.html)
