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

public class FloatValue extends ValueAdapter
{
    private final double val;

    public FloatValue( double val )
    {
        this.val = val;
    }

    @Override
    public String javaString()
    {
        return Double.toString( val );
    }

    @Override
    public int javaInteger()
    {
        return (int) val;
    }

    @Override
    public long javaLong()
    {
        return (long) val;
    }

    @Override
    public float javaFloat()
    {
        return (float) val;
    }

    @Override
    public boolean javaBoolean()
    {
        return val != 0;
    }

    @Override
    public double javaDouble()
    {
        return val;
    }

    @Override
    public boolean isFloat()
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

        FloatValue values = (FloatValue) o;

        return Double.compare( values.val, val ) == 0;

    }

    @Override
    public int hashCode()
    {
        long temp = Double.doubleToLongBits( val );
        return (int) (temp ^ (temp >>> 32));
    }
}
