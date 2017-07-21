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
package org.neo4j.kernel.api.impl.insight;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.apache.lucene.document.Field.Store.NO;

/**
 * Enumeration representing all possible property types with corresponding encodings and query structures for Lucene
 * schema indexes.
 */
enum InsightFieldEncoding
{
    Number
            {
                @Override
                String key()
                {
                    return "number";
                }

                @Override
                boolean canEncode( Value value )
                {
                    return Values.isNumberValue( value );
                }

                @Override
                Field encodeField( String name, Value value )
                {
                    throw new UnsupportedOperationException();
                }

//                @Override
//                void setFieldValue( Value value, Field field )
//                {
//                    field.setDoubleValue( Values.coerceToDouble( value ) );
//                }
//
//                @Override
//                Query encodeQuery( Value value, int propertyNumber )
//                {
//                    Double doubleValue = Values.coerceToDouble( value );
//                    return new ConstantScoreQuery( NumericRangeQuery
//                            .newDoubleRange( key( propertyNumber ), doubleValue, doubleValue, true, true ) );
//                }
            },
    Array
            {
                @Override
                String key()
                {
                    return "array";
                }

                @Override
                boolean canEncode( Value value )
                {
                    return Values.isArrayValue( value );
                }

                @Override
                Field encodeField( String name, Value value )
                {
                    throw new UnsupportedOperationException();
                }

//                @Override
//                void setFieldValue( Value value, Field field )
//                {
//                    field.setStringValue( ArrayEncoder.encode( value ) );
//                }
//
//                @Override
//                Query encodeQuery( Value value, int propertyNumber )
//                {
//                    return new ConstantScoreQuery(
//                            new TermQuery( new Term( key( propertyNumber ), ArrayEncoder.encode( value ) ) ) );
//                }
            },
    Bool
            {
                @Override
                String key()
                {
                    return "bool";
                }

                @Override
                boolean canEncode( Value value )
                {
                    return Values.isBooleanValue( value );
                }

                @Override
                Field encodeField( String name, Value value )
                {
                    throw new UnsupportedOperationException();
                }

//                @Override
//                void setFieldValue( Value value, Field field )
//                {
//                    field.setStringValue( value.prettyPrint() );
//                }
//
//                @Override
//                Query encodeQuery( Value value, int propertyNumber )
//                {
//                    return new ConstantScoreQuery(
//                            new TermQuery( new Term( key( propertyNumber ), value.prettyPrint() ) ) );
//                }
            },
    String
            {
                @Override
                String key()
                {
                    return "string";
                }

                @Override
                boolean canEncode( Value value )
                {
                    return Values.isTextValue( value );
                }

                @Override
                Field encodeField( String name, Value value )
                {
                    java.lang.String stringValue = ((TextValue) value).stringValue();

                    TokenStream tokenStream = englishAnalyzer.tokenStream( name, stringValue );
                    TextField field = new TextField( name, tokenStream );
                    return field;
                }

//                @Override
//                void setFieldValue( Value value, Field field )
//                {
//                    field.setStringValue( value.asObject().toString() );
//                }
//
//                @Override
//                Query encodeQuery( Value value, int propertyNumber )
//                {
//                    return new ConstantScoreQuery(
//                            new TermQuery( new Term( key( propertyNumber ), value.asObject().toString() ) ) );
//                }
            };

    private static EnglishAnalyzer englishAnalyzer = new EnglishAnalyzer();
    private static final InsightFieldEncoding[] AllEncodings = values();

    abstract String key();

    abstract boolean canEncode( Value value );

    abstract Field encodeField( String name, Value value );


    public static InsightFieldEncoding forValue( Value value )
    {
        for ( InsightFieldEncoding encoding : AllEncodings )
        {
            if ( encoding.canEncode( value ) )
            {
                return encoding;
            }
        }
        throw new IllegalStateException( "Unable to encode the value " + value );
    }
}
