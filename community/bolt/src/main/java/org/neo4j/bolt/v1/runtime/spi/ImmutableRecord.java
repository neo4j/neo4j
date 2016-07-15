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
package org.neo4j.bolt.v1.runtime.spi;

import java.util.Arrays;

public class ImmutableRecord implements Record
{
    private final Object[] fields;

    public ImmutableRecord( Object[] fields )
    {
        this.fields = fields;
    }

    @Override
    public Object[] fields()
    {
        return fields;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || !(o instanceof Record) )
        {
            return false;
        }

        Record that = (Record) o;
        if ( !Arrays.equals( fields, that.fields() ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public String toString()
    {
        return "ImmutableRecord{" +
                "fields=" + Arrays.toString( fields ) +
                '}';
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( fields );
    }
}
