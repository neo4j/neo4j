/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.store.prototype.records;

import org.neo4j.impl.kernel.api.EdgeTraversalCursor;
import org.neo4j.impl.kernel.api.NodeCursor;
import org.neo4j.impl.kernel.api.PropertyCursor;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class EdgeCursor extends RelationshipRecord implements EdgeTraversalCursor
{
    private final RelationshipStore store;
    long originNode;
    boolean scan;

    public EdgeCursor( RelationshipStore store )
    {
        super( -1 );
        this.store = store;
    }

    @Override
    public long edgeReference()
    {
        return getId();
    }

    @Override
    public int label()
    {
        return getType();
    }

    @Override
    public boolean hasProperties()
    {
        throw new UnsupportedOperationException( "not implemented" );
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
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long targetNodeReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long propertiesReference()
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
        return originNode;
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
    public boolean next()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean shouldRetry()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
