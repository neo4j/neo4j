/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index;

import java.util.Objects;

public class SCKey implements Comparable<SCKey>
{
    private long id;
    private long prop;

    public SCKey( long id, long prop )
    {
        this.id = id;
        this.prop = prop;
    }

    public long getId()
    {
        return id;
    }

    public long getProp()
    {
        return prop;
    }

    @Override
    public int compareTo( SCKey o )
    {
        Objects.requireNonNull( o );
        return id == o.id ? Long.compare( prop, o.prop ) : Long.compare( id, o.id );
    }

    @Override
    public int hashCode() {
        return (int) ( id * 23 + prop );
    }

    @Override
    public boolean equals( Object obj ) {
        if ( !( obj instanceof SCKey) )
            return false;
        if ( obj == this )
            return true;

        SCKey rhs = (SCKey) obj;
        return this.compareTo( rhs ) == 0;
    }

    @Override
    public String toString()
    {
        return String.format( "(%d,%d)", id, prop );
    }

}
