/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Validates {@link TextValue text values} so that they are within a certain length, byte-wise.
 */
public class IndexTextValueLengthValidator implements Validator<Value>
{
    private final int maxByteLength;
    private final int checkThreshold;

    public IndexTextValueLengthValidator( int maxByteLength )
    {
        this.maxByteLength = maxByteLength;

        // This check threshold is for not having to check every value that comes in, only those that may have a chance to exceed the max length.
        // The value 5 comes from a safer 4, which is the number of bytes that a max size UTF-8 code point needs.
        this.checkThreshold = maxByteLength / 5;
    }

    @Override
    public void validate( Value value )
    {
        if ( value == null || value == Values.NO_VALUE )
        {
            throw new IllegalArgumentException( "Null value" );
        }
        if ( Values.isTextValue( value ) && ((TextValue)value).length() >= checkThreshold )
        {
            validate( ((TextValue)value).stringValue().getBytes() );
        }
    }

    public void validate( byte[] encodedValue )
    {
        if ( encodedValue == null )
        {
            throw new IllegalArgumentException( "Null value" );
        }

        int byteLength = encodedValue.length;
        if ( byteLength > maxByteLength )
        {
            throw new IllegalArgumentException( "Property value bytes length: " + byteLength +
                    " is longer than " + maxByteLength + ", which is maximum supported length" +
                    " of indexed property value." );
        }
    }
}
