/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.neo4j.values.storable.Value;

import static org.apache.lucene.document.Field.Store.NO;

/**
 * Enumeration representing all possible property types with corresponding encodings and query structures for Lucene
 * schema indexes.
 */
public enum ValueEncoding
{
    String
            {
                @Override
                public String key()
                {
                    return "string";
                }

                @Override
                boolean canEncode( Value value )
                {
                    // Any other type can be safely serialised as a string
                    return true;
                }

                @Override
                Field encodeField( String name, Value value )
                {
                    return stringField( name, value.asObject().toString() );
                }

                @Override
                void setFieldValue( Value value, Field field )
                {
                    field.setStringValue( value.asObject().toString() );
                }

                @Override
                Query encodeQuery( Value value, int propertyNumber )
                {
                    return new ConstantScoreQuery( new TermQuery( new Term( key( propertyNumber ), value.asObject().toString() ) ) );
                }
            };

    private static final ValueEncoding[] AllEncodings = values();

    public abstract String key();

    String key( int propertyNumber )
    {
        if ( propertyNumber == 0 )
        {
            return key();
        }
        return propertyNumber + key();
    }

    static int fieldPropertyNumber( String fieldName )
    {
        int index = 0;
        for ( int i = 0; i < fieldName.length() && Character.isDigit( fieldName.charAt( i ) ); i++ )
        {
            index++;
        }
        return index == 0 ? 0 : Integer.parseInt( fieldName.substring( 0, index ) );
    }

    abstract boolean canEncode( Value value );

    abstract Field encodeField( String name, Value value );

    abstract void setFieldValue( Value value, Field field );

    abstract Query encodeQuery( Value value, int propertyNumber );

    public static ValueEncoding forValue( Value value )
    {
        for ( ValueEncoding encoding : AllEncodings )
        {
            if ( encoding.canEncode( value ) )
            {
                return encoding;
            }
        }
        throw new IllegalStateException( "Unable to encode the value " + value );
    }

    private static Field stringField( String identifier, String value )
    {
        return new StringField( identifier, value, NO );
    }
}
