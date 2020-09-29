/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.values.storable.Value;

public class StubNodeCursor extends DefaultCloseListenable implements NodeCursor
{
    private int offset = -1;
    private boolean dense;
    private List<NodeData> nodes = new ArrayList<>();
    private int degree;

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

    public StubNodeCursor withDegree( int degree )
    {
        this.degree = degree;
        return this;
    }

    @Override
    public long nodeReference()
    {
        return offset >= 0 && offset < nodes.size() ? nodes.get( offset ).id : -1;
    }

    @Override
    public TokenSet labels()
    {
        return offset >= 0 && offset < nodes.size() ? nodes.get( offset ).labelSet() : TokenSet.NONE;
    }

    @Override
    public TokenSet labelsIgnoringTxStateSetRemove()
    {
        return labels();
    }

    @Override
    public boolean hasLabel( int label )
    {
        return labels().contains( label );
    }

    @Override
    public void relationships( RelationshipTraversalCursor relationships, RelationshipSelection selection )
    {
        ((StubRelationshipCursor) relationships).rewind( nodeReference(), selection );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((StubPropertyCursor) cursor).init( nodes.get( offset ).properties );
    }

    @Override
    public long relationshipsReference()
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
    public boolean supportsFastDegreeLookup()
    {
        return dense;
    }

    @Override
    public int[] relationshipTypes()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Degrees degrees( RelationshipSelection selection )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public int degree( RelationshipSelection selection )
    {
        return degree;
    }

    @Override
    public int degreeWithMax( int maxDegree, RelationshipSelection selection )
    {
        return Math.min( maxDegree, this.degree );
    }

    @Override
    public void setTracer( KernelReadTracer tracer )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void removeTracer()
    {
        throw new UnsupportedOperationException( "not implemented" );
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
    public void closeInternal()
    {

    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

}
