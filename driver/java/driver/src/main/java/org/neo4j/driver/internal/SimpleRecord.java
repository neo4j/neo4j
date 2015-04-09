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
package org.neo4j.driver.internal;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;

public class SimpleRecord implements Record
{
    private final Map<String,Integer> fieldLookup;
    private final Value[] fields;

    public static Record record( Object... alternatingFieldNameValue )
    {
        Map<String,Integer> lookup = new HashMap<>();
        Value[] fields = new Value[alternatingFieldNameValue.length / 2];
        for ( int i = 0; i < alternatingFieldNameValue.length; i += 2 )
        {
            lookup.put( alternatingFieldNameValue[i].toString(), i / 2 );
            fields[i / 2] = (Value) alternatingFieldNameValue[i + 1];
        }
        return new SimpleRecord( lookup, fields );
    }

    public SimpleRecord( Map<String,Integer> fieldLookup, Value[] fields )
    {
        this.fieldLookup = fieldLookup;
        this.fields = fields;
    }

    @Override
    public Value get( int fieldIndex )
    {
        return fields[fieldIndex];
    }

    @Override
    public Value get( String fieldName )
    {
        return fields[fieldLookup.get( fieldName )];
    }

    @Override
    public Iterable<String> fieldNames()
    {
        return fieldLookup.keySet();
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

        SimpleRecord that = (SimpleRecord) o;

        if ( !fieldLookup.equals( that.fieldLookup ) )
        {
            return false;
        }
        if ( !Arrays.equals( fields, that.fields ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = fieldLookup.hashCode();
        result = 31 * result + Arrays.hashCode( fields );
        return result;
    }
}
