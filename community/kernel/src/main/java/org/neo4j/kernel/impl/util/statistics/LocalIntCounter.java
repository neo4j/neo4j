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
 * Used as a local counter, which manages its own counter as well as delegating changes to a global counter.
 */
public class LocalIntCounter extends IntCounter
{
    private final IntCounter global;

    public LocalIntCounter( IntCounter globalCounter )
    {
        this.global = globalCounter;
    }

    @Override
    public void increment()
    {
        super.increment();
        global.increment();
    }

    @Override
    public void decrement()
    {
        super.decrement();
        global.decrement();
    }

    @Override
    public void clear()
    {
        super.clear();
    }

    @Override
    public void add( int delta )
    {
        super.add( delta );
        global.add( delta );
    }

    @Override
    public String toString()
    {
        return "local:" + super.toString() + ",global:" + global.toString();
    }
}
