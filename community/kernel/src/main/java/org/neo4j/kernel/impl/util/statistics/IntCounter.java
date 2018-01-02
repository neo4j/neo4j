/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util.statistics;

/**
 * A wrapper for a primitive int counter, allowing it to be passed around different components.
 */
public class IntCounter
{
    private int count = 0;

    public int value()
    {
        return count;
    }

    public void increment()
    {
        count++;
    }

    public void decrement()
    {
        count--;
    }

    public void clear()
    {
        this.count = 0;
    }

    @Override
    public String toString()
    {
        return Integer.toString( count );
    }

    public void add( int delta )
    {
        count += delta;
    }
}
