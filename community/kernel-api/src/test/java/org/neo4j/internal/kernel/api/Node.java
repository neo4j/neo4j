package org.neo4j.internal.kernel.api;

import java.util.Map;

import org.neo4j.values.storable.Value;

class Node
{
    final long id;
    private final long[] labels;
    final Map<Integer,Value> properties;

    Node( long id, long[] labels, Map<Integer,Value> properties )
    {
        this.id = id;
        this.labels = labels;
        this.properties = properties;
    }

    LabelSet labelSet()
    {
        return new LabelSet()
        {
            @Override
            public int numberOfLabels()
            {
                return labels.length;
            }

            @Override
            public int label( int offset )
            {
                return labels.length;
            }

            @Override
            public boolean contains( int labelToken )
            {
                for ( long label : labels )
                {
                    if ( label == labelToken )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public long[] all()
            {
                return labels;
            }
        };
    }
}
