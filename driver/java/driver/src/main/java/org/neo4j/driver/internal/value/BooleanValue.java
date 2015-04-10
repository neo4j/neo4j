/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.value;

public class BooleanValue extends ValueAdapter
{
    private final boolean val;

    public BooleanValue( boolean val )
    {
        this.val = val;
    }

    @Override
    public boolean javaBoolean()
    {
        return val;
    }

    @Override
    public String javaString()
    {
        return val ? "true" : "false";
    }

    @Override
    public int javaInteger()
    {
        return val ? 1 : 0;
    }

    @Override
    public long javaLong()
    {
        return val ? 1 : 0;
    }

    @Override
    public float javaFloat()
    {
        return val ? 1 : 0;
    }

    @Override
    public double javaDouble()
    {
        return val ? 1 : 0;
    }

    @Override
    public boolean isBoolean()
    {
        return true;
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

        BooleanValue values = (BooleanValue) o;

        return val == values.val;

    }

    @Override
    public int hashCode()
    {
        return javaInteger();
    }
}
