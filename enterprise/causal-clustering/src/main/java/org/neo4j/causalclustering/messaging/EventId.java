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
package org.neo4j.causalclustering.messaging;

import java.util.Objects;
import java.util.UUID;

public class EventId
{
    public static EventId from( String id )
    {
        return new EventId( id );
    }

    public static EventId create()
    {
        return new EventId( UUID.randomUUID().toString().substring( 0, 5 ) );
    }

    private final String id;

    private EventId( String id )
    {
        this.id = id;
    }

    @Override
    public String toString()
    {
        return id;
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
        EventId eventId = (EventId) o;
        return Objects.equals( id, eventId.id );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( id );
    }
}
