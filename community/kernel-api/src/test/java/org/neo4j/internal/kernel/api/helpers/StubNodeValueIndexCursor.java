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

import java.util.Iterator;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.values.storable.Value;

public class StubNodeValueIndexCursor implements NodeValueIndexCursor
{
    private Iterator<Pair<Long,Value[]>> things;
    private Pair<Long,Value[]> current;

    public StubNodeValueIndexCursor( Iterator<Pair<Long,Value[]>> things )
    {
        this.things = things;
    }

    @Override
    public void node( NodeCursor cursor )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long nodeReference()
    {
        return current.first();
    }

    @Override
    public boolean next()
    {
        if ( things.hasNext() )
        {
            current = things.next();
            return true;
        }
        return false;
    }

    @Override
    public int numberOfProperties()
    {
        return current.other().length;
    }

    @Override
    public int propertyKey( int offset )
    {
        return 0;
    }

    @Override
    public boolean hasValue()
    {
        return current.other() != null;
    }

    @Override
    public Value propertyValue( int offset )
    {
        return current.other()[offset];
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
