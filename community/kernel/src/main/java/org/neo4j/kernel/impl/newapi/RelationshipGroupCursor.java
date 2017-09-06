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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

class RelationshipGroupCursor extends RelationshipGroupRecord
        implements org.neo4j.internal.kernel.api.RelationshipGroupCursor
{
    private final Read read;

    RelationshipGroupCursor( Read read )
    {
        super( -1 );
        this.read = read;
    }

    void init( NeoStores stores, long nodeReference, long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
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
        return false;
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int relationshipLabel()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int outgoingCount()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int incomingCount()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int loopCount()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long outgoingReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long incomingReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long loopsReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
