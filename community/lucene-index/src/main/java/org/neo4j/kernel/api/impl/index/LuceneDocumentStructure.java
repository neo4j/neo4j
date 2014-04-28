/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.neo4j.kernel.api.index.ArrayEncoder;

import static java.lang.String.format;

import static org.apache.lucene.document.Field.Index.NOT_ANALYZED;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;
import static org.apache.lucene.util.NumericUtils.doubleToPrefixCoded;
import static org.apache.lucene.util.NumericUtils.longToPrefixCoded;

import static org.neo4j.kernel.api.index.ArrayEncoder.asDoubleArray;
import static org.neo4j.kernel.api.index.ArrayEncoder.asLongArray;
import static org.neo4j.kernel.api.index.ArrayEncoder.isFloatingPointValueAndCanCoerceCleanlyIntoLong;

public class LuceneDocumentStructure
{
    static final String NODE_ID_KEY = "id";

    Document newDocument( long nodeId )
    {
        Document document = new Document();
        document.add( field( NODE_ID_KEY, "" + nodeId, YES ) );
        return document;
    }

    enum ValueEncoding
    {
        /**
         * Indexing numbers is a little tricky because we do coercion between numeric types for the purpose of checking
         * equality. All the integer types can coerce cleanly to long without loss, and all integer types except long
         * can coerce cleanly to double without loss, and 32 bit floats can coerce to doubles without loss. However,
         * long values cannot always be coerced to a double without loosing precision. This introduces a number of
         * cases that we need to guard for.
         *
         * We solve this by always indexing the double representation for longs, so that when we query with a double
         * value that that long value can be coerced into, we will find it. And likewise, we also index the long
         * representation for any double value that can cleanly be coerced to a long value without loss of precision
         * or information.
         *
         * So we have this decision table for indexing numbers:
         *
         *   Numeric Coercion : Indexed document
         *    D -> L -> D     = (long=L, number=D)
         *    D +> L          = (number=D)
         *    L               = (long=L, number=D)
         *
         * And we have this table for constructing our queries:
         *
         *   Numeric Coercion : Query
         *    D -> L -> D     = (long=L)
         *    D +> L          = (number=D)
         *    L               = (long=L)
         *
         * Where -> means precise coercion is possible, and +> means precise coercion is not possible.
         *
         * A similar thing is done for arrays, where we index both representations if any element of the array needs
         * to be represented both as a long and as a double, according to the rules above.
         */
        Long
        {
            @Override
            String key()
            {
                return "long";
            }

            @Override
            boolean canEncode( Object value )
            {
                return value instanceof Long || isFloatingPointValueAndCanCoerceCleanlyIntoLong( value );
            }

            @Override
            void encodeIntoDocument( Object value, Document document )
            {
                document.add( field( key(), longToPrefixCoded( ((Number)value).longValue() ) ) );
                Number.encodeIntoDocument( value, document );
            }

            @Override
            Query encodeQuery( Object value )
            {
                String encodedString = longToPrefixCoded( ((Number)value).longValue() );
                return new TermQuery( new Term( key(), encodedString ) );
            }
        },
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
            void encodeIntoDocument( Object value, Document document )
            {
                document.add( field( key(), doubleToPrefixCoded( ((Number)value).doubleValue() ) ) );
            }

            @Override
            Query encodeQuery( Object value )
            {
                String encodedString = doubleToPrefixCoded( ((Number)value).doubleValue() );
                return new TermQuery( new Term( key(), encodedString ) );
            }
        },
        Long_Array
        {
            @Override
            String key()
            {
                return "long_array";
            }

            @Override
            boolean canEncode( Object value )
            {
                // true if any item in the array can be encoded by Long encoder
                if ( value.getClass().isArray() )
                {
                    int length = java.lang.reflect.Array.getLength( value );
                    for ( int i = 0; i < length; i++ )
                    {
                        if ( Long.canEncode( java.lang.reflect.Array.get( value, i ) ) )
                        {
                            return true;
                        }
                    }
                }
                return false;
            }

            @Override
            void encodeIntoDocument( Object value, Document document )
            {
                document.add( field( key(), ArrayEncoder.encode( asLongArray( value ) ) ) );
                document.add( field( Array.key(), ArrayEncoder.encode( asDoubleArray( value ) ) ) );
            }

            @Override
            Query encodeQuery( Object value )
            {
                return new TermQuery( new Term( key(), ArrayEncoder.encode( asLongArray( value ) ) ) );
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
            void encodeIntoDocument( Object value, Document document )
            {
                document.add( field( key(), ArrayEncoder.encode( value ) ) );
            }

            @Override
            Query encodeQuery( Object value )
            {
                return new TermQuery( new Term( key(), ArrayEncoder.encode( value ) ) );
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
            void encodeIntoDocument( Object value, Document document )
            {
                document.add( field( key(), value.toString() ) );
            }

            @Override
            Query encodeQuery( Object value )
            {
                return new TermQuery( new Term( key(), value.toString() ) );
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
            void encodeIntoDocument( Object value, Document document )
            {
                document.add( field( key(), value.toString() ) );
            }

            @Override
            Query encodeQuery( Object value )
            {
                return new TermQuery( new Term( key(), value.toString() ) );
            }
        };

        abstract String key();

        abstract boolean canEncode( Object value );
        abstract void encodeIntoDocument( Object value, Document document );
        abstract Query encodeQuery( Object value );
    }

    public Document newDocumentRepresentingProperty( long nodeId, Object value )
    {
        Document document = newDocument( nodeId );

        for ( ValueEncoding encoding : ValueEncoding.values() )
        {
            if ( encoding.canEncode( value ) )
            {
                encoding.encodeIntoDocument( value, document );
                break;
            }
        }

        return document;
    }

    private static Field field( String fieldIdentifier, String value )
    {
        return field( fieldIdentifier, value, NO );
    }

    private static Field field( String fieldIdentifier, String value, Field.Store store )
    {
        Field result = new Field( fieldIdentifier, value, store, NOT_ANALYZED );
        result.setOmitNorms( true );
        result.setIndexOptions( IndexOptions.DOCS_ONLY );
        return result;
    }

    public Query newQuery( Object value )
    {
        for ( ValueEncoding encoding : ValueEncoding.values() )
        {
            if ( encoding.canEncode( value ) )
            {
                Query query = encoding.encodeQuery( value );
                System.out.println( "Query for " + value + "::" + value.getClass().getSimpleName() + " = " + query );
                return query;
            }
        }
        throw new IllegalArgumentException( format( "Unable to create newQuery for %s", value ) );
    }

    public Term newQueryForChangeOrRemove( long nodeId )
    {
        return new Term( NODE_ID_KEY, "" + nodeId );
    }

    public long getNodeId( Document from )
    {
        return Long.parseLong( from.get( NODE_ID_KEY ) );
    }
}
