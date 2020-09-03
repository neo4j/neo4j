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
package org.neo4j.util;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class Id
{
    protected final UUID uuid;
    private String shortName;

    public Id( UUID uuid )
    {
        requireNonNull( uuid, "UUID should be not null." );
        this.uuid = uuid;
    }

    public UUID uuid()
    {
        return uuid;
    }

    protected String shortName()
    {
        if ( shortName == null )
        {
            shortName = uuid.toString().substring( 0, 8 );
        }
        return shortName;
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
        Id id = (Id) o;
        return Objects.equals( uuid, id.uuid );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( uuid );
    }

    @Override
    public String toString()
    {
        return shortName();
    }
}
