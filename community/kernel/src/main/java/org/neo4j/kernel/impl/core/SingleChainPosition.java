/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.function.primitive.FunctionFromPrimitiveLongLongToPrimitiveLong;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

public class SingleChainPosition implements RelationshipLoadingPosition
{
    private long position;

    public SingleChainPosition( long firstPosition )
    {
        this.position = firstPosition;
    }

    @Override
    public long position( DirectionWrapper direction, int[] types )
    {
        return this.position;
    }

    @Override
    public long nextPosition( long nextRel, DirectionWrapper direction, int[] types )
    {
        this.position = nextRel;
        return nextRel;
    }

    @Override
    public boolean hasMore( DirectionWrapper direction, int[] types )
    {
        return position != Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    @Override
    public boolean atPosition( DirectionWrapper direction, int type, long position )
    {
        return this.position == position;
    }

    @Override
    public void patchPosition( long nodeId, FunctionFromPrimitiveLongLongToPrimitiveLong<RuntimeException> next )
    {
        position = next.apply( nodeId, position );
    }

    @Override
    public RelationshipLoadingPosition clone()
    {
        return new SingleChainPosition( position );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + position + "]";
    }
}
