/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.kernel.api.helpers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.values.storable.Value;

public class StubNodeCursor implements NodeCursor
{
    private int offset = -1;
    private boolean dense;
    private List<NodeData> nodes = new ArrayList<>();

    public StubNodeCursor()
    {
        this( true );
    }

    public StubNodeCursor( boolean dense )
    {
        this.dense = dense;
    }

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
        nodes.add( new NodeData( id, new long[]{}, Collections.emptyMap() ) );
        return this;
    }

    public StubNodeCursor withNode( long id, long... labels )
    {
        nodes.add( new NodeData( id, labels, Collections.emptyMap() ) );
        return this;
    }

    public StubNodeCursor withNode( long id, long[] labels, Map<Integer,Value> properties )
    {
        nodes.add( new NodeData( id, labels, properties ) );
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
        ((StubGroupCursor) cursor).rewind();
    }

    @Override
    public void allRelationships( RelationshipTraversalCursor relationships )
    {
        ((StubRelationshipCursor) relationships).rewind();
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
            NodeData node = nodes.get( offset );
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
        return dense;
    }

    @Override
    public boolean next()
    {
        if ( offset == Integer.MAX_VALUE )
        {
            return false;
        }
        return ++offset < nodes.size();
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

}
