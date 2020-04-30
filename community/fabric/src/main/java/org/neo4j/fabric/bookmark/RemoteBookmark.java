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
package org.neo4j.fabric.bookmark;

import java.util.Objects;

/**
 * A bookmark received after interacting with a remote graph.
 */
public class RemoteBookmark
{
    private final String serialisedState;

    public RemoteBookmark( String serialisedState )
    {
        this.serialisedState = serialisedState;
    }

    public String getSerialisedState()
    {
        return serialisedState;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        RemoteBookmark that = (RemoteBookmark) o;
        return serialisedState.equals( that.serialisedState );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( serialisedState );
    }
}
