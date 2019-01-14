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

import java.util.Map;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;

public class StubNodeLabelIndexCursor implements NodeLabelIndexCursor
{
    private int offset = -1;
    private final Map<Integer,long[]> lookup;
    private int label;

    public StubNodeLabelIndexCursor( Map<Integer,long[]> lookup )
    {
        this.lookup = lookup;
    }

    void initalize( int label )
    {
        this.label = label;
        this.offset = -1;
    }

    @Override
    public void node( NodeCursor cursor )
    {

    }

    @Override
    public long nodeReference()
    {
        long[] nodes = lookup.get( label );
        if ( nodes == null )
        {
            return -1L;
        }

        return offset >= 0 && offset < nodes.length ? nodes[offset] : -1;
    }

    @Override
    public LabelSet labels()
    {
        return null;
    }

    @Override
    public boolean next()
    {
        long[] nodes = lookup.get( label );
        return nodes != null && ++offset < nodes.length;
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
