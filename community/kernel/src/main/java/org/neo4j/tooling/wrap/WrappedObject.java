/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.tooling.wrap;

abstract class WrappedObject<W>
{
    final WrappedGraphDatabase graphdb;
    final W wrapped;

    WrappedObject( WrappedGraphDatabase graphdb, W wrapped )
    {
        this.graphdb = graphdb;
        this.wrapped = wrapped;
    }

    @Override
    public final int hashCode()
    {
        return wrapped.hashCode();
    }

    @Override
    public final boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( getClass().isInstance( obj ) )
        {
            WrappedObject<?> other = (WrappedObject<?>) obj;
            if ( wrapped == null ? other.wrapped == null : wrapped.equals( other.wrapped ) )
                return graphdb.equals( other.graphdb );
        }
        return false;
    }

    @Override
    public String toString()
    {
        return wrapped.toString();
    }
}
