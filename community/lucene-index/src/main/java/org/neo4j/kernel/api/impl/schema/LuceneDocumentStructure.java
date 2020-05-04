/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;

import org.neo4j.util.FeatureToggles;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.apache.lucene.document.Field.Store.YES;

public class LuceneDocumentStructure
{
    private static final boolean USE_LUCENE_STANDARD_PREFIX_QUERY = FeatureToggles.flag( LuceneDocumentStructure.class, "lucene.standard.prefix.query", false );

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
            builder.add( ValueEncoding.String.encodeQuery( values[i], i ), BooleanClause.Occur.MUST );
        }
        return builder.build();
    }

    public static Query newRangeSeekByStringQuery( String lower, boolean includeLower, String upper, boolean includeUpper )
    {
        boolean includeLowerBoundary = StringUtils.EMPTY.equals( lower ) || includeLower;
        boolean includeUpperBoundary = StringUtils.EMPTY.equals( upper ) || includeUpper;
        TermRangeQuery termRangeQuery =
                TermRangeQuery.newStringRange( ValueEncoding.String.key( 0 ), lower, upper, includeLowerBoundary, includeUpperBoundary );

        if ( (includeLowerBoundary != includeLower) || (includeUpperBoundary != includeUpper) )
        {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            if ( includeLowerBoundary != includeLower )
            {
                builder.add( new TermQuery( new Term( ValueEncoding.String.key( 0 ), lower ) ), BooleanClause.Occur.MUST_NOT );
            }
            if ( includeUpperBoundary != includeUpper )
            {
                builder.add( new TermQuery( new Term( ValueEncoding.String.key( 0 ), upper ) ), BooleanClause.Occur.MUST_NOT );
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
        return USE_LUCENE_STANDARD_PREFIX_QUERY ? new PrefixQuery( term ) : new PrefixMultiTermsQuery( term );
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
     * Simple implementation of prefix query that mimics old lucene way of handling prefix queries.
     * According to benchmarks this implementation is faster then {@link PrefixQuery} because we do
     * not construct automaton which is extremely expensive.
     */
    public static class PrefixMultiTermsQuery extends MultiTermQuery
    {
        private Term term;

        public PrefixMultiTermsQuery( Term term )
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

    public static boolean useFieldForUniquenessVerification( String fieldName )
    {
        return !LuceneDocumentStructure.NODE_ID_KEY.equals( fieldName ) && ValueEncoding.fieldPropertyNumber( fieldName ) == 0;
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
            document.clear();
            document.add( idField );
            document.add( idValueField );
        }

        private Field getFieldWithValue( int propertyNumber, Value value )
        {
            if ( value.valueGroup() != ValueGroup.TEXT )
            {
                throw new IllegalArgumentException( "Only text values can be stored in a Lucene index, but we tried to store: " + value );
            }
            int reuseId = propertyNumber * ValueEncoding.values().length + ValueEncoding.String.ordinal();
            String key = ValueEncoding.String.key( propertyNumber );
            Field reusableField = reusableValueFields[reuseId];
            if ( reusableField == null )
            {
                reusableField = ValueEncoding.String.encodeField( key, value );
                reusableValueFields[reuseId] = reusableField;
            }
            else
            {
                ValueEncoding.String.setFieldValue( value, reusableField );
            }
            return reusableField;
        }
    }
}
