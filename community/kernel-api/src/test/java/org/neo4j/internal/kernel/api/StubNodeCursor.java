/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.values.storable.Value;

public class StubNodeCursor implements NodeCursor
{
    private int offset = -1;
    private List<Node> nodes = new ArrayList<>();

    void single( long reference )
    {
        offset = Integer.MAX_VALUE;
        for ( int i = 0; i < nodes.size(); i++ )
        {
            if ( reference == nodes.get( i ).id )
            {
                offset = i - 1;
            }
        }
    }

    void scan()
    {
        offset = -1;
    }

    public StubNodeCursor withNode( long id )
    {
        nodes.add( new Node( id, new long[]{}, Collections.emptyMap() ) );
        return this;
    }

    public StubNodeCursor withNode( long id, long... labels )
    {
        nodes.add( new Node( id, labels, Collections.emptyMap() ) );
        return this;
    }

    public StubNodeCursor withNode( long id, long[] labels, Map<Integer,Value> properties )
    {
        nodes.add( new Node( id, labels, properties ) );
        return this;
    }

    @Override
    public long nodeReference()
    {
        return offset >= 0 && offset < nodes.size() ? nodes.get( offset ).id : -1;
    }

    @Override
    public LabelSet labels()
    {
        return offset >= 0 && offset < nodes.size() ? nodes.get( offset ).labelSet() : LabelSet.NONE;
    }

    @Override
    public boolean hasProperties()
    {
        return (offset >= 0 && offset < nodes.size()) && !nodes.get( offset ).properties.isEmpty();
    }

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor relationships )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((StubPropertyCursor) cursor).init( nodes.get( offset ).properties );
    }

    @Override
    public long relationshipGroupReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long allRelationshipsReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long propertiesReference()
    {
        if ( offset >= 0 && offset < nodes.size() )
        {
            Node node = nodes.get( offset );
            if ( !node.properties.isEmpty() )
            {
                return node.id;
            }
        }
        return -1;
    }

    @Override
    public boolean isDense()
    {
        return false;
    }

    @Override
    public boolean next()
    {
        return ++offset < nodes.size();
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {

    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

    static class Node
    {
        final long id;
        final long[] labels;
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
            };
        }
    }
}
