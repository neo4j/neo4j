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

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;

class RelationshipManualIndexCursor implements org.neo4j.internal.kernel.api.RelationshipManualIndexCursor
{
    private final Read read;

    RelationshipManualIndexCursor( Read read )
    {
        this.read = read;
    }

    @Override
    public int totalExpectedCursorSize()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public float score()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationship( RelationshipScanCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void sourceNode( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void targetNode( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int relationshipLabel()
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
}
