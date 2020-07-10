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

import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.NO_VALUE;

public class StubNodeValueIndexCursor extends DefaultCloseListenable implements NodeValueIndexCursor
{
    private int position = -1;
    private final List<NodeData> nodes = new ArrayList<>();
    private List<Value[]> values = new ArrayList<>();

    public StubNodeValueIndexCursor withNode( long id, Value... vs )
    {
        nodes.add( new NodeData( id, new long[]{}, Collections.emptyMap() ) );
        values.add( vs );
        return this;
    }
    @Override
    public void node( NodeCursor cursor )
    {

    }

    @Override
    public long nodeReference()
    {
        return position >= 0 && position < nodes.size() ? nodes.get( position ).id : -1;
    }

    @Override
    public float score()
    {
        return Float.NaN;
    }

    @Override
    public boolean next()
    {
        return ++position < nodes.size();
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

    @Override
    public void setTracer( KernelReadTracer tracer )
    {
    }

    @Override
    public void removeTracer()
    {
    }

    @Override
    public int numberOfProperties()
    {
        return position >= 0 && position < values.size() ? values.get( position ).length : 0;
    }

    @Override
    public int propertyKey( int offset )
    {
        return 0;
    }

    @Override
    public boolean hasValue()
    {
        return values != null;
    }

    @Override
    public Value propertyValue( int offset )
    {
        return position >= 0 && position < values.size() ? values.get( position )[offset] : NO_VALUE;
    }
}
