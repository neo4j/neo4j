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
package org.neo4j.kernel.logging;

import java.util.Iterator;

import org.slf4j.Marker;

public class Slf4jMarkerAdapter implements Marker
{
    private final String name;

    public Slf4jMarkerAdapter( String name )
    {
        this.name = name;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void add( Marker reference )
    {
    }

    @Override
    public boolean remove( Marker reference )
    {
        return false;
    }

    @Override
    public boolean hasChildren()
    {
        return false;
    }

    @Override
    public boolean hasReferences()
    {
        return false;
    }

    @Override
    public Iterator iterator()
    {
        return null;
    }

    @Override
    public boolean contains( Marker other )
    {
        return false;
    }

    @Override
    public boolean contains( String name )
    {
        return false;
    }
}
