package org.neo4j.server.rrd;

import org.neo4j.kernel.AbstractGraphDatabase;

import javax.management.MalformedObjectNameException;

public class NodeIdsInUseSampleable extends JmxSampleableBase
{
    public NodeIdsInUseSampleable( AbstractGraphDatabase graphDb )
            throws MalformedObjectNameException
    {
        super( graphDb );
    }

    public String getName()
    {
        return "node_count";
    }

    protected String getJmxAttributeName()
    {
        return "NumberOfNodeIdsInUse";
    }
}
