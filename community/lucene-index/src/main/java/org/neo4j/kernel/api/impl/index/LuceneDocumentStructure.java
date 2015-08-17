/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import org.neo4j.kernel.api.index.ArrayEncoder;

import static java.lang.String.format;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;

public class LuceneDocumentStructure
{
    static final String NODE_ID_KEY = "id";

    Document newDocument( long nodeId )
    {
        Document document = new Document();
        document.add( field( NODE_ID_KEY, "" + nodeId, YES ) );
        document.add( new NumericDocValuesField( NODE_ID_KEY, nodeId ) );
        return document;
    }

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
            IndexableField encodeField( Object value )
            {
                return new DoubleField( key(), ((Number) value).doubleValue(), NO );
            }

            @Override
            NumericRangeQuery<Double> encodeQuery( Object value )
            {
                final Double doubleValue = ((Number) value).doubleValue();
                return NumericRangeQuery.newDoubleRange( key(), doubleValue, doubleValue, true, true );
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
            IndexableField encodeField( Object value )
            {
                return field( key(), ArrayEncoder.encode( value ) );
            }

            @Override
            TermQuery encodeQuery( Object value )
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
            IndexableField encodeField( Object value )
            {
                return field( key(), value.toString() );
            }

            @Override
            TermQuery encodeQuery( Object value )
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
            IndexableField encodeField( Object value )
            {
                return field( key(), value.toString() );
            }

            @Override
            TermQuery encodeQuery( Object value )
            {
                return new TermQuery( new Term( key(), value.toString() ) );
            }
        };

        abstract String key();

        abstract boolean canEncode( Object value );
        abstract IndexableField encodeField( Object value );
        abstract Query encodeQuery( Object value );

        public static ValueEncoding fromKey( String key )
        {
            switch ( key )
            {
            case "number":
                return Number;
            case "array":
                return Array;
            case "bool":
                return Bool;
            case "string":
                return String;
            }
            throw new IllegalArgumentException( "Unknown key: " + key );
        }
    }

    public Document newDocumentRepresentingProperty( long nodeId, IndexableField encodedValue )
    {
        Document document = newDocument( nodeId );
        document.add( encodedValue );
        return document;
    }

    public IndexableField encodeAsFieldable( Object value )
    {
        for ( ValueEncoding encoding : ValueEncoding.values() )
        {
            if ( encoding.canEncode( value ) )
            {
                return encoding.encodeField( value );
            }
        }

        throw new IllegalStateException( "Unable to encode the value " + value );
    }

    private static Field field( String fieldIdentifier, String value )
    {
        return field( fieldIdentifier, value, NO );
    }

    private static Field field( String fieldIdentifier, String value, Field.Store store )
    {
        return new StringField( fieldIdentifier, value, store );
    }

    public MatchAllDocsQuery newScanQuery()
    {
        return new MatchAllDocsQuery();
    }

    public Query newSeekQuery( Object value )
    {
        for ( ValueEncoding encoding : ValueEncoding.values() )
        {
            if ( encoding.canEncode( value ) )
            {
                return encoding.encodeQuery( value );
            }
        }
        throw new IllegalArgumentException( format( "Unable to create query for %s", value ) );
    }

    /**
     * Range queries are always inclusive, in order to do exclusive range queries the result must be filtered after the
     * fact. The reason we can't do inclusive range queries is that longs are coerced to doubles in the index.
     */
    public NumericRangeQuery<Double> newInclusiveNumericRangeSeekQuery( Number lower, Number upper ) {
        Double min = lower != null ? lower.doubleValue() : null;
        Double max = upper != null ? upper.doubleValue() : null;
        return NumericRangeQuery.newDoubleRange( ValueEncoding.Number.key(), min, max, true, true );
    }

    public TermRangeQuery newRangeSeekByStringQuery( String lower, boolean includeLower,
                                                     String upper, boolean upperInclusive )
    {
        BytesRef chosenLower = lower != null ? new BytesRef( lower ) : null;
        boolean includeChosenLower = lower == null || includeLower;

        BytesRef chosenUpper = upper != null ? new BytesRef( upper ) : null;
        boolean includeChosenUpper = upper == null || upperInclusive;

        return new TermRangeQuery(
                ValueEncoding.String.key(),
                chosenLower, chosenUpper,
                includeChosenLower, includeChosenUpper
        );
    }

    public PrefixQuery newRangeSeekByPrefixQuery( String prefix )
    {
        return new PrefixQuery( new Term( ValueEncoding.String.key(), prefix ) );
    }

    public Term newTermForChangeOrRemove( long nodeId )
    {
        return new Term( NODE_ID_KEY, "" + nodeId );
    }

    public long getNodeId( Document from )
    {
        return Long.parseLong( from.get( NODE_ID_KEY ) );
    }
}
