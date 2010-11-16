package org.neo4j.server.rrd;

import org.neo4j.kernel.AbstractGraphDatabase;

import javax.management.MalformedObjectNameException;

public class PropertyCountSampleable extends JmxSampleableBase
{
    public PropertyCountSampleable( AbstractGraphDatabase graphDb )
            throws MalformedObjectNameException
    {
        super( graphDb );
    }

    @Override
    public String getName()
    {
        return "property_count";
    }

    @Override
    protected String getJmxAttributeName()
    {
        return "NumberOfPropertyIdsInUse";
    }
}
