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

public class TextValue extends ValueAdapter
{
    private final String val;

    public TextValue( String val )
    {
        assert val != null;
        this.val = val;
    }

    @Override
    public boolean javaBoolean()
    {
        return !val.isEmpty();
    }

    @Override
    public String javaString()
    {
        return val;
    }

    @Override
    public boolean isText()
    {
        return true;
    }

    @Override
    public long size()
    {
        return val.length();
    }

    @Override
    public String toString()
    {
        return val;
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

        TextValue values = (TextValue) o;

        return val.equals( values.val );

    }

    @Override
    public int hashCode()
    {
        return val.hashCode();
    }
}
