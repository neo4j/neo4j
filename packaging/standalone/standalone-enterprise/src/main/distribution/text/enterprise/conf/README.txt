#{product.fullname} Configuration
=======================================

#{product.fullname} configuration files.

* neo4j-http-logging.xml             -- logging system configuration for the HTTP REST server
                                        using standard logback options
* arbiter-wrapper.conf               -- environment and launch settings for the arbiter instance
* jmx.access                         -- JMX access settings
* jmx.password                       -- JMX password
* neo4j-server.properties            -- runtime operational settings
* neo4j.properties                   -- database tuning parameters
* neo4j-wrapper.conf                 -- environment and launch settings for Neo4j Server
* ssl/                               -- directory holding certificate files for
                                        HTTPS operation of the REST server


References
----------

* [Java Logging Overview](http://docs.oracle.com/javase/7/docs/technotes/guides/logging/overview.html)
* [JMX](http://docs.oracle.com/javase/7/docs/technotes/guides/management/agent.html)
