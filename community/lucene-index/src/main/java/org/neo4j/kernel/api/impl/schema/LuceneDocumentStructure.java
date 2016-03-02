/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

import static org.apache.lucene.document.Field.Store.YES;

public class LuceneDocumentStructure
{
    private static final boolean USE_LUCENE_STANDARD_PREFIX_QUERY =
            FeatureToggles.flag( LuceneDocumentStructure.class, "lucene.standard.prefix.query", false );

    public static final String NODE_ID_KEY = "id";

    //  Absolute hard maximum length for a term, in bytes once
    //  encoded as UTF8.  If a term arrives from the analyzer
    //  longer than this length, an IllegalArgumentException
    //  when lucene writer trying to add or update document
    private static final int MAX_FIELD_LENGTH = IndexWriter.MAX_TERM_LENGTH;


    private static final ThreadLocal<DocWithId> perThreadDocument = new ThreadLocal<DocWithId>()
    {
        @Override
        protected DocWithId initialValue()
        {
            return new DocWithId( NODE_ID_KEY );
        }
    };

    private LuceneDocumentStructure()
    {
    }

    private static DocWithId reuseDocument( long nodeId )
    {
        DocWithId doc = perThreadDocument.get();
        doc.setId( nodeId );
        return doc;
    }

    public static Document documentRepresentingProperty( long nodeId, Object value )
    {
        DocWithId document = reuseDocument( nodeId );
        document.setValue( ValueEncoding.forValue( value ), value );
        return document.document;
    }

    public static String encodedStringValue( Object value )
    {
        ValueEncoding encoding = ValueEncoding.forValue( value );
        Field field = encoding.encodeField( value );
        return field.stringValue();
    }

    public static MatchAllDocsQuery newScanQuery()
    {
        return new MatchAllDocsQuery();
    }

    public static Query newSeekQuery( Object value )
    {
        ValueEncoding encoding = ValueEncoding.forValue( value );
        return encoding.encodeQuery( value );
    }

    /**
     * Range queries are always inclusive, in order to do exclusive range queries the result must be filtered after the
     * fact. The reason we can't do inclusive range queries is that longs are coerced to doubles in the index.
     */
    public static NumericRangeQuery<Double> newInclusiveNumericRangeSeekQuery( Number lower, Number upper )
    {
        Double min = lower != null ? lower.doubleValue() : null;
        Double max = upper != null ? upper.doubleValue() : null;
        return NumericRangeQuery.newDoubleRange( ValueEncoding.Number.key(), min, max, true, true );
    }

    public static Query newRangeSeekByStringQuery( String lower, boolean includeLower,
            String upper, boolean includeUpper )
    {
        boolean includeLowerBoundary = StringUtils.EMPTY.equals( lower ) || includeLower;
        boolean includeUpperBoundary = StringUtils.EMPTY.equals( upper ) || includeUpper;
        TermRangeQuery termRangeQuery = TermRangeQuery.newStringRange( ValueEncoding.String.key(), lower, upper,
                includeLowerBoundary, includeUpperBoundary );

        if ( (includeLowerBoundary != includeLower) || (includeUpperBoundary != includeUpper) )
        {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            if ( includeLowerBoundary != includeLower )
            {
                builder.add( new TermQuery( new Term( ValueEncoding.String.key(), lower ) ), BooleanClause.Occur
                        .MUST_NOT );
            }
            if ( includeUpperBoundary != includeUpper )
            {
                builder.add( new TermQuery( new Term( ValueEncoding.String.key(), upper ) ), BooleanClause.Occur
                        .MUST_NOT );
            }
            builder.add( termRangeQuery, BooleanClause.Occur.SHOULD );
            return builder.build();
        }
        return new ConstantScoreQuery( termRangeQuery );
    }

    public static Query newWildCardStringQuery( String searchFor )
    {
        String searchTerm = QueryParser.escape( searchFor );
        Term term = new Term( ValueEncoding.String.key(), "*" + searchTerm + "*" );

        return new WildcardQuery( term );
    }

    public static Query newRangeSeekByPrefixQuery( String prefix )
    {
        Term term = new Term( ValueEncoding.String.key(), prefix );
        MultiTermQuery prefixQuery = USE_LUCENE_STANDARD_PREFIX_QUERY ? new PrefixQuery( term ) :
                                     new PrefixMultiTermsQuery( term );
        return new ConstantScoreQuery( prefixQuery );
    }

    public static Query newSuffixStringQuery( String suffix )
    {
        String searchTerm = QueryParser.escape( suffix );
        Term term = new Term( ValueEncoding.String.key(), "*" + searchTerm );

        return new WildcardQuery( term );
    }

    public static Term newTermForChangeOrRemove( long nodeId )
    {
        return new Term( NODE_ID_KEY, "" + nodeId );
    }

    public static long getNodeId( Document from )
    {
        return Long.parseLong( from.get( NODE_ID_KEY ) );
    }

    /**
     * Filters the given {@link Terms terms} to include only terms that were created using fields from
     * {@link ValueEncoding#encodeField(Object)}. Internal lucene terms like those created for indexing numeric values
     * (see javadoc for {@link NumericRangeQuery} class) are skipped. In other words this method returns
     * {@link TermsEnum} over all terms for the given field that were created using {@link ValueEncoding}.
     *
     * @param terms the terms to be filtered
     * @param fieldKey the corresponding {@link ValueEncoding#key() field key}
     * @return terms enum over all inserted terms
     * @throws IOException if it is not possible to obtain {@link TermsEnum}
     * @see NumericRangeQuery
     * @see org.apache.lucene.analysis.NumericTokenStream
     * @see NumericUtils#PRECISION_STEP_DEFAULT
     * @see NumericUtils#filterPrefixCodedLongs(TermsEnum)
     */
    public static TermsEnum originalTerms( Terms terms, String fieldKey ) throws IOException
    {
        TermsEnum termsEnum = terms.iterator();
        return ValueEncoding.forKey( fieldKey ) == ValueEncoding.Number
               ? NumericUtils.filterPrefixCodedLongs( termsEnum )
               : termsEnum;
    }

    /**
     * Simple implementation of prefix query that mimics old lucene way of handling prefix queries.
     * According to benchmarks this implementation is faster then
     * {@link org.apache.lucene.search.PrefixQuery} because we do not construct automaton  which is
     * extremely expensive.
     */
    private static class PrefixMultiTermsQuery extends MultiTermQuery
    {
        private Term term;

        PrefixMultiTermsQuery( Term term )
        {
            super(term.field());
            this.term = term;
        }

        @Override
        protected TermsEnum getTermsEnum( Terms terms, AttributeSource atts ) throws IOException
        {
            return term.bytes().length == 0 ? terms.iterator() : new PrefixTermsEnum( terms.iterator(), term.bytes() );
        }


        @Override
        public String toString( String field )
        {
            return getClass().getSimpleName() + ", term:" + term + ", field:" + field;
        }

        private class PrefixTermsEnum extends FilteredTermsEnum
        {
            private BytesRef prefix;

            PrefixTermsEnum( TermsEnum termEnum, BytesRef prefix )
            {
                super( termEnum );
                this.prefix = prefix;
                setInitialSeekTerm( this.prefix );
            }

            @Override
            protected AcceptStatus accept( BytesRef term ) throws IOException
            {
                return StringHelper.startsWith( term, prefix ) ? AcceptStatus.YES : AcceptStatus.END;
            }
        }
    }

    private static class DocWithId
    {
        private final Document document;

        private final String idFieldName;
        private final Field idField;
        private final Field idValueField;

        private final Map<ValueEncoding,Field> valueFields = new EnumMap<>( ValueEncoding.class );

        private DocWithId( String idFieldName )
        {
            this.idFieldName = idFieldName;
            idField = new StringField( idFieldName, "", YES );
            idValueField = new NumericDocValuesField( idFieldName, 0L );
            document = new Document();
            document.add( idField );
            document.add( idValueField );
        }

        private void setId( long id )
        {
            idField.setStringValue( "" + id );
            idValueField.setLongValue( id );
        }

        private void setValue( ValueEncoding encoding, Object value )
        {
            removeAllValueFields();
            Field reusableField = getFieldWithValue( encoding, value );
            if ( isArrayOrString( reusableField ) )
            {
                if ( isShorterThenMaximum( reusableField ) )
                {
                    document.add( reusableField );
                }
            }
            else
            {
                document.add( reusableField );
            }
        }

        private boolean isShorterThenMaximum( Field reusableField )
        {
            return reusableField.stringValue().getBytes().length <= MAX_FIELD_LENGTH;
        }

        private boolean isArrayOrString( Field reusableField )
        {
            return ValueEncoding.Array.key().equals( reusableField.name() ) ||
                   ValueEncoding.String.key().equals( reusableField.name() );
        }

        private void removeAllValueFields()
        {
            Iterator<IndexableField> it = document.getFields().iterator();
            while ( it.hasNext() )
            {
                IndexableField field = it.next();
                String fieldName = field.name();
                if ( !fieldName.equals( idFieldName ) )
                {
                    it.remove();
                }
            }
        }

        private Field getFieldWithValue( ValueEncoding encoding, Object value )
        {
            Field reusableField = valueFields.get( encoding );
            if ( reusableField == null )
            {
                reusableField = encoding.encodeField( value );
                valueFields.put( encoding, reusableField );
            }
            else
            {
                encoding.setFieldValue( value, reusableField );
            }
            return reusableField;
        }
    }
}
