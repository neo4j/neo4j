/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com;

import java.util.Iterator;
import java.util.NoSuchElementException;

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
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        return start < end ? next++ : next--;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
