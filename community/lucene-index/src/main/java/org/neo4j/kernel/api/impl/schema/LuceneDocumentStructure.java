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

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.FilteredTermsEnum;
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
import java.util.Iterator;

import org.neo4j.util.FeatureToggles;
import org.neo4j.values.storable.Value;

import static org.apache.lucene.document.Field.Store.YES;

public class LuceneDocumentStructure
{
    private static final boolean USE_LUCENE_STANDARD_PREFIX_QUERY =
            FeatureToggles.flag( LuceneDocumentStructure.class, "lucene.standard.prefix.query", false );

    public static final String NODE_ID_KEY = "id";

    private static final ThreadLocal<DocWithId> perThreadDocument = ThreadLocal.withInitial( DocWithId::new );
    public static final String DELIMITER = "\u001F";

    private LuceneDocumentStructure()
    {
    }

    private static DocWithId reuseDocument( long nodeId )
    {
        DocWithId doc = perThreadDocument.get();
        doc.setId( nodeId );
        return doc;
    }

    public static Document documentRepresentingProperties( long nodeId, Value... values )
    {
        DocWithId document = reuseDocument( nodeId );
        document.setValues( values );
        return document.document;
    }

    public static String encodedStringValuesForSampling( Value... values )
    {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for ( Value value : values )
        {
            sb.append( sep );
            sep = DELIMITER;
            ValueEncoding encoding = ValueEncoding.forValue( value );
            sb.append( encoding.encodeField( encoding.key(), value ).stringValue() );
        }
        return sb.toString();
    }

    public static MatchAllDocsQuery newScanQuery()
    {
        return new MatchAllDocsQuery();
    }

    public static Query newSeekQuery( Value... values )
    {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for ( int i = 0; i < values.length; i++ )
        {
            ValueEncoding encoding = ValueEncoding.forValue( values[i] );
            builder.add( encoding.encodeQuery( values[i], i ), BooleanClause.Occur.MUST );
        }
        return builder.build();
    }

    /**
     * Range queries are always inclusive, in order to do exclusive range queries the result must be filtered after the
     * fact. The reason we can't do inclusive range queries is that longs are coerced to doubles in the index.
     */
    public static NumericRangeQuery<Double> newInclusiveNumericRangeSeekQuery( Number lower, Number upper )
    {
        Double min = lower != null ? lower.doubleValue() : null;
        Double max = upper != null ? upper.doubleValue() : null;
        return NumericRangeQuery.newDoubleRange( ValueEncoding.Number.key( 0 ), min, max, true, true );
    }

    public static Query newRangeSeekByStringQuery( String lower, boolean includeLower,
            String upper, boolean includeUpper )
    {
        boolean includeLowerBoundary = StringUtils.EMPTY.equals( lower ) || includeLower;
        boolean includeUpperBoundary = StringUtils.EMPTY.equals( upper ) || includeUpper;
        TermRangeQuery termRangeQuery = TermRangeQuery.newStringRange( ValueEncoding.String.key( 0 ), lower, upper,
                includeLowerBoundary, includeUpperBoundary );

        if ( (includeLowerBoundary != includeLower) || (includeUpperBoundary != includeUpper) )
        {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.setDisableCoord(true);
            if ( includeLowerBoundary != includeLower )
            {
                builder.add( new TermQuery( new Term( ValueEncoding.String.key( 0 ), lower ) ), BooleanClause.Occur
                        .MUST_NOT );
            }
            if ( includeUpperBoundary != includeUpper )
            {
                builder.add( new TermQuery( new Term( ValueEncoding.String.key( 0 ), upper ) ), BooleanClause.Occur
                        .MUST_NOT );
            }
            builder.add( termRangeQuery, BooleanClause.Occur.FILTER );
            return new ConstantScoreQuery( builder.build() );
        }
        return termRangeQuery;
    }

    public static Query newWildCardStringQuery( String searchFor )
    {
        String searchTerm = QueryParser.escape( searchFor );
        Term term = new Term( ValueEncoding.String.key( 0 ), "*" + searchTerm + "*" );

        return new WildcardQuery( term );
    }

    public static Query newRangeSeekByPrefixQuery( String prefix )
    {
        Term term = new Term( ValueEncoding.String.key( 0 ), prefix );
        return USE_LUCENE_STANDARD_PREFIX_QUERY ? new PrefixQuery( term ) :
                                     new PrefixMultiTermsQuery( term );
    }

    public static Query newSuffixStringQuery( String suffix )
    {
        String searchTerm = QueryParser.escape( suffix );
        Term term = new Term( ValueEncoding.String.key( 0 ), "*" + searchTerm );

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
     * {@link ValueEncoding#encodeField(String, Value)}. Internal lucene terms like those created for indexing numeric values
     * (see javadoc for {@link NumericRangeQuery} class) are skipped. In other words this method returns
     * {@link TermsEnum} over all terms for the given field that were created using {@link ValueEncoding}.
     *
     * @param terms the terms to be filtered
     * @param fieldKey the corresponding {@link ValueEncoding#key(int) field key}
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
            super( term.field() );
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

        private static class PrefixTermsEnum extends FilteredTermsEnum
        {
            private BytesRef prefix;

            PrefixTermsEnum( TermsEnum termEnum, BytesRef prefix )
            {
                super( termEnum );
                this.prefix = prefix;
                setInitialSeekTerm( this.prefix );
            }

            @Override
            protected AcceptStatus accept( BytesRef term )
            {
                return StringHelper.startsWith( term, prefix ) ? AcceptStatus.YES : AcceptStatus.END;
            }
        }
    }

    public static Field encodeValueField( Value value )
    {
        ValueEncoding encoding = ValueEncoding.forValue( value );
        return encoding.encodeField( encoding.key(), value );
    }

    public static boolean useFieldForUniquenessVerification( String fieldName )
    {
        return !LuceneDocumentStructure.NODE_ID_KEY.equals( fieldName ) &&
                ValueEncoding.fieldPropertyNumber( fieldName ) == 0;
    }

    private static class DocWithId
    {
        private final Document document;

        private final Field idField;
        private final Field idValueField;

        private Field[] reusableValueFields = new Field[0];

        private DocWithId()
        {
            idField = new StringField( NODE_ID_KEY, "", YES );
            idValueField = new NumericDocValuesField( NODE_ID_KEY, 0L );
            document = new Document();
            document.add( idField );
            document.add( idValueField );
        }

        private void setId( long id )
        {
            idField.setStringValue( Long.toString( id ) );
            idValueField.setLongValue( id );
        }

        private void setValues( Value... values )
        {
            removeAllValueFields();
            int neededLength = values.length * ValueEncoding.values().length;
            if ( reusableValueFields.length < neededLength )
            {
                reusableValueFields = new Field[neededLength];
            }

            for ( int i = 0; i < values.length; i++ )
            {
                Field reusableField = getFieldWithValue( i, values[i] );
                document.add( reusableField );
            }
        }

        private void removeAllValueFields()
        {
            Iterator<IndexableField> it = document.getFields().iterator();
            while ( it.hasNext() )
            {
                IndexableField field = it.next();
                String fieldName = field.name();
                if ( !fieldName.equals( NODE_ID_KEY ) )
                {
                    it.remove();
                }
            }
        }

        private Field getFieldWithValue( int propertyNumber, Value value )
        {
            ValueEncoding encoding = ValueEncoding.forValue( value );
            int reuseId = propertyNumber * ValueEncoding.values().length + encoding.ordinal();
            String key = encoding.key( propertyNumber );
            Field reusableField = reusableValueFields[reuseId];
            if ( reusableField == null )
            {
                reusableField = encoding.encodeField( key, value );
                reusableValueFields[reuseId] = reusableField;
            }
            else
            {
                encoding.setFieldValue( value, reusableField );
            }
            return reusableField;
        }
    }
}
