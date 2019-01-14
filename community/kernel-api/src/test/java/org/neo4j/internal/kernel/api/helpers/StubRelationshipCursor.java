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

import java.util.Collections;
import java.util.List;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

public class StubRelationshipCursor implements RelationshipTraversalCursor
{
    private final List<TestRelationshipChain> store;

    private int offset;
    private int chainId;
    private boolean isClosed;

    public StubRelationshipCursor( TestRelationshipChain chain )
    {
        this( Collections.singletonList( chain ) );
    }

    StubRelationshipCursor( List<TestRelationshipChain> store )
    {
        this.store = store;
        this.chainId = 0;
        this.offset = -1;
        this.isClosed = true;
    }

    void rewind()
    {
        this.offset = -1;
        this.isClosed = true;
    }

    void read( int chainId )
    {
        this.chainId = chainId;
        rewind();
    }

    @Override
    public long relationshipReference()
    {
        return store.get( chainId ).get( offset ).id;
    }

    @Override
    public int type()
    {
        return store.get( chainId ).get( offset ).type;
    }

    @Override
    public boolean hasProperties()
    {
        return false;
    }

    @Override
    public void source( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void target( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long sourceNodeReference()
    {
        return store.get( chainId ).get( offset ).source;
    }

    @Override
    public long targetNodeReference()
    {
        return store.get( chainId ).get( offset ).target;
    }

    @Override
    public long propertiesReference()
    {
        return -1;
    }

    @Override
    public Position suspend()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void resume( Position position )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void neighbour( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long neighbourNodeReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long originNodeReference()
    {
        return store.get( chainId ).originNodeId();
    }

    @Override
    public boolean next()
    {
        offset++;
        return store.get( chainId ).isValidOffset( offset );
    }

    @Override
    public void close()
    {
        isClosed = true;
    }

    @Override
    public boolean isClosed()
    {
        return isClosed;
    }
}
