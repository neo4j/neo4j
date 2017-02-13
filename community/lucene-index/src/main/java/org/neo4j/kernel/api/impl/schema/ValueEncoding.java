/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.neo4j.kernel.api.index.ArrayEncoder;

import static org.apache.lucene.document.Field.Store.NO;

/**
 * Enumeration representing all possible property types with corresponding encodings and query structures for Lucene
 * schema indexes.
 */
enum ValueEncoding
{
    Number
            {
                @Override
                String key()
                {
                    return "number";
                }

                @Override
                boolean canEncode( Object value )
                {
                    return value instanceof Number;
                }

                @Override
                Field encodeField( String name, Object value )
                {
                    return new DoubleField( name, ((Number) value).doubleValue(), NO );
                }

                @Override
                void setFieldValue( Object value, Field field )
                {
                    field.setDoubleValue( ((Number) value).doubleValue() );
                }

                @Override
                Query encodeQuery( Object value )
                {
                    Double doubleValue = ((Number) value).doubleValue();
                    return new ConstantScoreQuery( NumericRangeQuery.newDoubleRange( key(), doubleValue, doubleValue,
                            true, true ) );
                }
            },
    Array
            {
                @Override
                String key()
                {
                    return "array";
                }

                @Override
                boolean canEncode( Object value )
                {
                    return value.getClass().isArray();
                }

                @Override
                Field encodeField( String name, Object value )
                {
                    return stringField( name, ArrayEncoder.encode( value ) );
                }

                @Override
                void setFieldValue( Object value, Field field )
                {
                    field.setStringValue( ArrayEncoder.encode( value ) );
                }

                @Override
                Query encodeQuery( Object value )
                {
                    return new ConstantScoreQuery( new TermQuery( new Term( key(), ArrayEncoder.encode( value ) ) ) );
                }
            },
    Bool
            {
                @Override
                String key()
                {
                    return "bool";
                }

                @Override
                boolean canEncode( Object value )
                {
                    return value instanceof Boolean;
                }

                @Override
                Field encodeField( String name, Object value )
                {
                    return stringField( name, value.toString() );
                }

                @Override
                void setFieldValue( Object value, Field field )
                {
                    field.setStringValue( value.toString() );
                }

                @Override
                Query encodeQuery( Object value )
                {
                    return new ConstantScoreQuery( new TermQuery( new Term( key(), value.toString() ) ) );
                }
            },
    String
            {
                @Override
                String key()
                {
                    return "string";
                }

                @Override
                boolean canEncode( Object value )
                {
                    // Any other type can be safely serialised as a string
                    return true;
                }

                @Override
                Field encodeField( String name, Object value )
                {
                    return stringField( name, value.toString() );
                }

                @Override
                void setFieldValue( Object value, Field field )
                {
                    field.setStringValue( value.toString() );
                }

                @Override
                Query encodeQuery( Object value )
                {
                    return new ConstantScoreQuery( new TermQuery( new Term( key(), value.toString() ) ) );
                }
            };

    private static final ValueEncoding[] AllEncodings = values();

    abstract String key();

    abstract boolean canEncode( Object value );

    abstract Field encodeField( String name, Object value );

    abstract void setFieldValue( Object value, Field field );

    abstract Query encodeQuery( Object value );

    public static ValueEncoding forKey( String key )
    {
        for ( ValueEncoding encoding : AllEncodings )
        {
            if ( encoding.key().equals( key ) )
            {
                return encoding;
            }
        }
        throw new IllegalArgumentException( "Unknown key: " + key );
    }

    public static ValueEncoding forValue( Object value )
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
