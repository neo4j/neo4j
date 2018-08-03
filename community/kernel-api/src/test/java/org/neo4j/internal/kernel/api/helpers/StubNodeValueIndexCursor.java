/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.List;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.values.storable.Value;

public class StubNodeValueIndexCursor implements NodeValueIndexCursor
{
    Iterator<Pair<Long,List<Value>>> things;
    Pair<Long,List<Value>> current = null;

    public StubNodeValueIndexCursor( Iterator<Pair<Long,List<Value>>> things )
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
        return current.other().size();
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
        return current.other().get( offset );
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
