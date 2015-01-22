/**
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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.NumericUtils;

import org.neo4j.graphdb.Lookup;
import org.neo4j.kernel.api.Specialization;
import org.neo4j.kernel.api.index.ArrayEncoder;

import static java.lang.String.format;

import static org.apache.lucene.document.Field.Index.NOT_ANALYZED;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;

public class LuceneDocumentStructure implements Lookup.Transformation<Query>, SpecializedQuery.User<Query>
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
            Fieldable encodeField( Object value )
            {
                String encodedString = NumericUtils.doubleToPrefixCoded( ((Number)value).doubleValue() );
                return field( key(), encodedString );
            }

            @Override
            Query encodeQuery( Object value )
            {
                String encodedString = NumericUtils.doubleToPrefixCoded( ((Number)value).doubleValue() );
                return new TermQuery( new Term( key(), encodedString ) );
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
            Fieldable encodeField( Object value )
            {
                return field( key(), ArrayEncoder.encode( value ) );
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
            Fieldable encodeField( Object value )
            {
                return field( key(), value.toString() );
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
            Fieldable encodeField( Object value )
            {
                return field( key(), value.toString() );
            }

            @Override
            Query encodeQuery( Object value )
            {
                return new TermQuery( new Term( key(), value.toString() ) );
            }
        };

        abstract String key();

        abstract boolean canEncode( Object value );
        abstract Fieldable encodeField( Object value );
        abstract Query encodeQuery( Object value );
    }

    public Document newDocumentRepresentingProperty( long nodeId, Fieldable encodedValue )
    {
        Document document = newDocument( nodeId );
        document.add( encodedValue );
        if ( ValueEncoding.String.key().equals( encodedValue.name() ) )
        {
            document.add( field( "reversed", new StringBuilder( encodedValue.stringValue() ).reverse().toString() ) );
        }
        return document;
    }

    public Fieldable encodeAsFieldable( Object value )
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
                return encoding.encodeQuery( value );
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

    // Specialization

    @Override
    public Query specialize( Lookup lookup )
    {
        return lookup.transform( this );
    }

    @Override
    public Query verify( SpecializedQuery specialized )
    {
        return specialized.structure == this ? specialized.query : specialize( specialized.genericForm() );
    }

    // Lookup.Transformation

    @SuppressWarnings("unchecked")
    final Lookup.Transformation<Specialization<Lookup>> queryTransformation =
            (Lookup.Transformation) SpecializedQuery.transformation( this );

    @Override
    public Query equalTo( Object value )
    {
        return newQuery( value );
    }

    @Override
    public Query startsWith( String prefix )
    {
        return new PrefixQuery( new Term( ValueEncoding.String.key(), prefix ) );
    }

    @Override
    public Query endsWith( String suffix )
    {
        return new PrefixQuery( new Term( "reversed", new StringBuilder( suffix ).reverse().toString() ) );
    }

//    @Override
//    public Query matches( String pattern )
//    {
//        throw new UnsupportedOperationException( "not implemented" );
//    }

    @Override
    public Query lessThan( Number value )
    {
        return range( false, Double.NEGATIVE_INFINITY, value, false );
    }

    @Override
    public Query lessThanOrEqualTo( Number value )
    {
        return range( false, Double.NEGATIVE_INFINITY, value, true );
    }

    @Override
    public Query greaterThan( Number value )
    {
        return range( false, value, Double.POSITIVE_INFINITY, false );
    }

    @Override
    public Query greaterThanOrEqualTo( Number value )
    {
        return range( true, value, Double.POSITIVE_INFINITY, false );
    }

    @Override
    public Query range( boolean includeLower, Number lower, Number upper, boolean includeUpper )
    {
        return new TermRangeQuery( ValueEncoding.Number.key(),
                                   NumericUtils.doubleToPrefixCoded( lower.doubleValue() ),
                                   NumericUtils.doubleToPrefixCoded( upper.doubleValue() ),
                                   includeLower, includeUpper );
    }

    @Override
    public Query not( Query lookup )
    {
        if ( lookup instanceof TermRangeQuery )
        {
            TermRangeQuery rangeQuery = (TermRangeQuery) lookup;
            BooleanQuery query = new BooleanQuery();
            query.add( new TermRangeQuery( ValueEncoding.Number.key(),
                                           NumericUtils.doubleToPrefixCoded( Double.NEGATIVE_INFINITY ),
                                           rangeQuery.getLowerTerm(),
                                           false, !rangeQuery.includesLower() ),
                       BooleanClause.Occur.SHOULD );
            query.add( new TermRangeQuery( ValueEncoding.Number.key(),
                                           rangeQuery.getUpperTerm(),
                                           NumericUtils.doubleToPrefixCoded( Double.POSITIVE_INFINITY ),
                                           !rangeQuery.includesUpper(), false ),
                       BooleanClause.Occur.SHOULD );
            query.setMinimumNumberShouldMatch( 1 );
            return query;
        }
        else
        {
            throw new UnsupportedOperationException(
                    "Cannot negate: " + genericForm( lookup ) +
                    ". Support for negated queries has not been implemented, lucene does not support it natively." );
        }
    }

    Lookup genericForm( Query query )
    {
        if ( query instanceof BooleanQuery )
        {
            BooleanClause[] clauses = ((BooleanQuery) query).getClauses();
            if ( clauses.length == 2 && clauses[0].getOccur() == BooleanClause.Occur.SHOULD &&
                 clauses[1].getOccur() == BooleanClause.Occur.SHOULD &&
                 clauses[0].getQuery() instanceof TermRangeQuery &&
                 clauses[1].getQuery() instanceof TermRangeQuery )
            {
                TermRangeQuery lower = (TermRangeQuery) clauses[0].getQuery();
                TermRangeQuery upper = (TermRangeQuery) clauses[1].getQuery();
                if ( ValueEncoding.Number.key().equals( lower.getField() ) &&
                     ValueEncoding.Number.key().equals( upper.getField() ) )
                {
                    if ( NumericUtils.doubleToPrefixCoded( Double.NEGATIVE_INFINITY ).equals( lower.getLowerTerm() ) )
                    {
                        TermRangeQuery tmp = lower;
                        lower = upper;
                        upper = tmp;
                    }
                    double lLow = NumericUtils.prefixCodedToDouble( lower.getLowerTerm() );
                    double lUpp = NumericUtils.prefixCodedToDouble( lower.getUpperTerm() );
                    double uLow = NumericUtils.prefixCodedToDouble( upper.getLowerTerm() );
                    double uUpp = NumericUtils.prefixCodedToDouble( upper.getUpperTerm() );
                    if ( lLow == Double.NEGATIVE_INFINITY && uUpp == Double.POSITIVE_INFINITY )
                    {
                        Lookup.LowerBound bound = lower.includesUpper() ? Lookup.greaterThan( lUpp )
                                                                        : Lookup.greaterThanOrEqualTo( lUpp );
                        return upper.includesLower() ? bound.andLessThan( uLow ) : bound.andLessThanOrEqualTo( uLow );
                    }
                }
            }
        }
        else if ( query instanceof TermRangeQuery )
        {
            TermRangeQuery rangeQuery = (TermRangeQuery) query;
            if ( ValueEncoding.Number.key().equals( rangeQuery.getField() ) )
            {
                double lower = NumericUtils.prefixCodedToDouble( rangeQuery.getLowerTerm() );
                double upper = NumericUtils.prefixCodedToDouble( rangeQuery.getUpperTerm() );
                Lookup.LowerBound bound = rangeQuery.includesLower()
                                          ? Lookup.greaterThanOrEqualTo( lower )
                                          : Lookup.greaterThan( lower );
                return rangeQuery.includesUpper() ? bound.andLessThanOrEqualTo( upper ) : bound.andLessThan( upper );
            }
        }
        else if ( query instanceof PrefixQuery )
        {
            Term prefix = ((PrefixQuery) query).getPrefix();
            if ( "reversed".equals( prefix.field() ) )
            {
                return Lookup.endsWith( new StringBuilder( prefix.text() ).reverse().toString() );
            }
            else if ( "string".equals( prefix.field() ) )
            {
                return Lookup.startsWith( prefix.text() );
            }
        }
        else if ( query instanceof TermQuery )
        {
            Term term = ((TermQuery) query).getTerm();
            for ( ValueEncoding encoding : ValueEncoding.values() )
            {
                if ( encoding.key().equals( term.field() ) )
                {
                    switch ( encoding )
                    {
                    case Number:
                        return Lookup.equalTo( NumericUtils.prefixCodedToDouble( term.text() ) );
                    case Bool:
                        return Lookup.equalTo( Boolean.parseBoolean( term.text() ) );
                    case String:
                        return Lookup.equalTo( term.text() );
                    }
                }
            }
        }
        throw new UnsupportedOperationException( query + " not recognized" );
    }
}
