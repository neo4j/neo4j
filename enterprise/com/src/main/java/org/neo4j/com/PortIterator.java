/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.com;

import java.util.Iterator;

public class PortIterator implements Iterator<Integer>
{
    private final int start;
    private final int end;
    private int next;

    public PortIterator( int[] portRanges )
    {
        start = portRanges[0];
        end = portRanges[1];
        next = start;
    }

    @Override
    public boolean hasNext()
    {
        return start < end ? next <= end : next >= end;
    }

    @Override
    public Integer next()
    {
        return start < end ? next++ : next--;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
