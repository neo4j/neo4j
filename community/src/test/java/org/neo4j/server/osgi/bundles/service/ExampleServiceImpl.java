package org.neo4j.server.osgi.bundles.service;

import org.neo4j.server.osgi.services.ExampleBundleService;

public class ExampleServiceImpl implements ExampleBundleService
{
    public String doSomethingWith( String someInput )
    {
        return "thanks for " + someInput;
    }
}
