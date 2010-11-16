package org.neo4j.server.rrd;

import org.neo4j.kernel.AbstractGraphDatabase;

import javax.management.MalformedObjectNameException;

public class RelationshipCountSampleable extends JmxSampleableBase
{
    public RelationshipCountSampleable( AbstractGraphDatabase graphDb )
            throws MalformedObjectNameException
    {
        super( graphDb );
    }

    @Override
    public String getName()
    {
        return "relationship_count";
    }

    @Override
    protected String getJmxAttributeName()
    {
        return "NumberOfRelationshipIdsInUse";
    }
}
