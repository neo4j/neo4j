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
package org.neo4j.index.impl.lucene.explicit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.neo4j.index.impl.lucene.explicit.LuceneExplicitIndex.isValidKey;

public abstract class IndexType
{
    public static final IndexType EXACT = new IndexType( LuceneDataSource.KEYWORD_ANALYZER, false )
    {
        @Override
        public Query get( String key, Object value )
        {
            return queryForGet( key, value );
        }

        @Override
        void removeFieldsFromDocument( Document document, String key, Object value )
        {
            removeFieldsFromDocument( document, key, key, value );
        }

        @Override
        protected void addNewFieldToDocument( Document document, String key, Object value )
        {
            document.add( instantiateField( key, value, StringField.TYPE_STORED ) );
            document.add( instantiateSortField( key, value ) );
        }

        @Override
        void removeFieldFromDocument( Document document, String name )
        {
            document.removeFields( name );
        }

        @Override
        public String toString()
        {
            return "EXACT";
        }
    };

    private static class CustomType extends IndexType
    {
        public static final String EXACT_FIELD_SUFFIX = "_e";
        private final Similarity similarity;

        CustomType( Analyzer analyzer, boolean toLowerCase, Similarity similarity )
        {
            super( analyzer, toLowerCase );
            this.similarity = similarity;
        }

        @Override
        Similarity getSimilarity()
        {
            return this.similarity;
        }

        @Override
        public Query get( String key, Object value )
        {
            // TODO we do value.toString() here since initially #addToDocument didn't
            // honor ValueContext, and changing it would mean changing store format.
            return new TermQuery( new Term( exactKey( key ), value.toString() ) );
        }

        private String exactKey( String key )
        {
            return key + EXACT_FIELD_SUFFIX;
        }

        // TODO We should honor ValueContext instead of doing value.toString() here.
        // if changing it, also change #get to honor ValueContext.
        @Override
        protected void addNewFieldToDocument( Document document, String key, Object value )
        {
            document.add( new StringField( exactKey( key ), value.toString(), Store.YES ) );
            document.add( instantiateField( key, value, TextField.TYPE_STORED ) );
            document.add( instantiateSortField( key, value ) );
        }

        @Override
        void removeFieldFromDocument( Document document, String name )
        {
            document.removeFields( exactKey( name ) );
            document.removeFields( name );
        }

        @Override
        void removeFieldsFromDocument( Document document, String key, Object value )
        {
            removeFieldsFromDocument( document, key, exactKey( key ), value );
        }

        @Override
        protected boolean isStoredField( IndexableField field )
        {
            return !field.name().endsWith( CustomType.EXACT_FIELD_SUFFIX ) && super.isStoredField( field );
        }

        @Override
        public String toString()
        {
            return "FULLTEXT";
        }
    }

    final Analyzer analyzer;
    private final boolean toLowerCase;

    private IndexType( Analyzer analyzer, boolean toLowerCase )
    {
        this.analyzer = analyzer;
        this.toLowerCase = toLowerCase;
    }

    abstract void removeFieldsFromDocument( Document document, String key, Object value );

    abstract void removeFieldFromDocument( Document document, String name );

    abstract void addNewFieldToDocument( Document document, String key, Object value );

    abstract Query get( String key, Object value );

    static IndexType getIndexType( Map<String, String> config )
    {
        String type = config.get( LuceneIndexImplementation.KEY_TYPE );
        IndexType result = null;
        Similarity similarity = getCustomSimilarity( config );
        Boolean toLowerCaseUnbiased = config.get( LuceneIndexImplementation.KEY_TO_LOWER_CASE ) != null ?
                                      parseBoolean( config.get( LuceneIndexImplementation.KEY_TO_LOWER_CASE ), true ) : null;
        Analyzer customAnalyzer = getCustomAnalyzer( config );
        if ( type != null )
        {
            // Use the built in alternatives... "exact" or "fulltext"
            if ( "exact".equals( type ) )
            {
                // In the exact case we default to false
                boolean toLowerCase = TRUE.equals( toLowerCaseUnbiased );

                result = toLowerCase ? new CustomType( new LowerCaseKeywordAnalyzer(), true, similarity ) : EXACT;
            }
            else if ( "fulltext".equals( type ) )
            {
                // In the fulltext case we default to true
                boolean toLowerCase = !FALSE.equals( toLowerCaseUnbiased );

                Analyzer analyzer = customAnalyzer;
                if ( analyzer == null )
                {
                    analyzer = TRUE.equals( toLowerCase ) ? LuceneDataSource.LOWER_CASE_WHITESPACE_ANALYZER :
                               LuceneDataSource.WHITESPACE_ANALYZER;
                }
                result = new CustomType( analyzer, toLowerCase, similarity );
            }
            else
            {
                throw new IllegalArgumentException( "The given type was not recognized: " + type +
                        ". Known types are 'fulltext' and 'exact'" );
            }
        }
        else
        {
            // In the custom case we default to true
            boolean toLowerCase = !FALSE.equals( toLowerCaseUnbiased );

            // Use custom analyzer
            if ( customAnalyzer == null )
            {
                throw new IllegalArgumentException( "No 'type' was given (which can point out " +
                        "built-in analyzers, such as 'exact' and 'fulltext')" +
                        " and no 'analyzer' was given either (which can point out a custom " +
                        Analyzer.class.getName() + " to use)" );
            }
            result = new CustomType( customAnalyzer, toLowerCase, similarity );
        }
        return result;
    }

    public void addToDocument( Document document, String key, Object value )
    {
        addNewFieldToDocument( document, key, value );
        restoreSortFields( document );
    }

    protected boolean isStoredField( IndexableField field )
    {
        return isValidKey( field.name() ) &&
                field.fieldType().stored() && !FullTxData.TX_STATE_KEY.equals( field.name() );
    }

    private static boolean parseBoolean( String string, boolean valueIfNull )
    {
        return string == null ? valueIfNull : Boolean.parseBoolean( string );
    }

    private static Similarity getCustomSimilarity( Map<String, String> config )
    {
        return getByClassName( config, LuceneIndexImplementation.KEY_SIMILARITY, Similarity.class );
    }

    private static Analyzer getCustomAnalyzer( Map<String, String> config )
    {
        return getByClassName( config, LuceneIndexImplementation.KEY_ANALYZER, Analyzer.class );
    }

    private static <T> T getByClassName( Map<String, String> config, String configKey, Class<T> cls )
    {
        String className = config.get( configKey );
        if ( className != null )
        {
            try
            {
                return Class.forName( className ).asSubclass( cls ).newInstance();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
        return null;
    }

    TxData newTxData( LuceneExplicitIndex index )
    {
        return new ExactTxData( index );
    }

    Query query( String keyOrNull, Object value, QueryContext contextOrNull )
    {
        if ( value instanceof Query )
        {
            return (Query) value;
        }

        QueryParser parser = new QueryParser( keyOrNull, analyzer );
        parser.setAllowLeadingWildcard( true );
        parser.setLowercaseExpandedTerms( toLowerCase );
        if ( contextOrNull != null && contextOrNull.getDefaultOperator() != null )
        {
            parser.setDefaultOperator( contextOrNull.getDefaultOperator() );
        }
        try
        {
            return parser.parse( value.toString() );
        }
        catch ( ParseException e )
        {
            throw new RuntimeException( e );
        }
    }

    public static IndexableField instantiateField( String key, Object value, FieldType fieldType )
    {
        IndexableField field;
        if ( value instanceof Number )
        {
            Number number = (Number) value;
            if ( value instanceof Long )
            {
                field = new LongField( key, number.longValue(), Store.YES );
            }
            else if ( value instanceof Float )
            {
                field = new FloatField( key, number.floatValue(), Store.YES );
            }
            else if ( value instanceof Double )
            {
                field = new DoubleField( key, number.doubleValue(), Store.YES );
            }
            else
            {
                field = new IntField( key, number.intValue(), Store.YES );
            }
        }
        else
        {
            field = new Field( key, value.toString(), fieldType );
        }
        return field;
    }

    public static IndexableField instantiateSortField( String key, Object value )
    {
        IndexableField field;
        if ( value instanceof Number )
        {
            Number number = (Number) value;
            if ( value instanceof Float )
            {
                field = new SortedNumericDocValuesField( key, NumericUtils.floatToSortableInt( number.floatValue() ) );
            }
            else if ( value instanceof Double )
            {
                field = new SortedNumericDocValuesField( key, NumericUtils.doubleToSortableLong( number.doubleValue() ) );
            }
            else
            {
                field = new SortedNumericDocValuesField( key, number.longValue() );
            }
        }
        else
        {
            if ( LuceneExplicitIndex.KEY_DOC_ID.equals( key ) )
            {
                field = new NumericDocValuesField( key, Long.parseLong( value.toString() ) );
            }
            else
            {
                field = new SortedSetDocValuesField( key, new BytesRef( value.toString() ) );
            }
        }
        return field;
    }

    final void removeFromDocument( Document document, String key, Object value )
    {
        if ( key == null && value == null )
        {
            clearDocument( document );
        }
        else
        {
            removeFieldsFromDocument( document, key, value );
            restoreSortFields( document );
        }
    }

    private void clearDocument( Document document )
    {
        Set<String> names = new HashSet<>();
        for ( IndexableField field : document.getFields() )
        {
            String name = field.name();
            if ( LuceneExplicitIndex.isValidKey( name ) )
            {
                names.add( name );
            }
        }
        for ( String name : names )
        {
            document.removeFields( name );
        }
    }

    // Re-add field since their index info is lost after reading the fields from the index store
    void restoreSortFields( Document document )
    {
        Collection<IndexableField> notIndexedStoredFields = getNotIndexedStoredFields( document );
        for ( IndexableField field : notIndexedStoredFields )
        {
            Object fieldValue = getFieldValue( field );
            String name = field.name();
            removeFieldsFromDocument( document, name, fieldValue );
            addNewFieldToDocument( document, name, fieldValue );
        }
    }

    private Collection<IndexableField> getNotIndexedStoredFields( Document document )
    {
        Map<String,IndexableField> nameFieldMap = new HashMap<>();
        List<String> indexedFields = new ArrayList<>();
        for ( IndexableField field : document.getFields() )
        {
            if ( isStoredField( field ) )
            {
                nameFieldMap.put( field.name(), field );
            }
            else if ( !DocValuesType.NONE.equals( field.fieldType().docValuesType() ) )
            {
                indexedFields.add( field.name() );
            }
        }
        indexedFields.forEach( nameFieldMap::remove );
        return nameFieldMap.values();
    }

    void removeFieldsFromDocument( Document document, String key, String exactKey, Object value )
    {
        Set<String> values = null;
        if ( value != null )
        {
            String stringValue = value.toString();
            values = new HashSet<>( Arrays.asList( document.getValues( exactKey ) ) );
            if ( !values.remove( stringValue ) )
            {
                return;
            }
        }
        removeFieldFromDocument( document, key );

        if ( value != null )
        {
            for ( String existingValue : values )
            {
                addNewFieldToDocument( document, key, existingValue );
            }
        }
    }

    private Object getFieldValue( IndexableField field )
    {
        Number numericFieldValue = field.numericValue();
        return numericFieldValue != null ? numericFieldValue : field.stringValue();
    }

    public static Document newBaseDocument( long entityId )
    {
        Document doc = new Document();
        doc.add( new StringField( LuceneExplicitIndex.KEY_DOC_ID, "" + entityId, Store.YES ) );
        doc.add( new NumericDocValuesField( LuceneExplicitIndex.KEY_DOC_ID, entityId ) );
        return doc;
    }

    public static Document newDocument( EntityId entityId )
    {
        Document document = newBaseDocument( entityId.id() );
        entityId.enhance( document );
        return document;
    }

    public Term idTerm( long entityId )
    {
        return new Term( LuceneExplicitIndex.KEY_DOC_ID, "" + entityId );
    }

    Query idTermQuery( long entityId )
    {
        return new TermQuery( idTerm( entityId ) );
    }

    Similarity getSimilarity()
    {
        return null;
    }

    Query queryForGet( String key, Object value )
    {
        if ( value instanceof ValueContext )
        {
            Object realValue = ((ValueContext)value).getValue();
            if ( realValue instanceof Number )
            {
                Number number = (Number) realValue;
                return LuceneUtil.rangeQuery( key, number, number, true, true );
            }
        }
        return new TermQuery( new Term( key, value.toString() ) );
    }
}
