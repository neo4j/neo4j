package org.neo4j.index.impl.lucene;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;

abstract class IndexType
{
    private static final IndexType EXACT = new IndexType( LuceneDataSource.KEYWORD_ANALYZER )
    {
        @Override
        public Query deletionQuery( long entityId, String key, Object value )
        {
            BooleanQuery q = new BooleanQuery();
            q.add( idTermQuery( entityId ), Occur.MUST );
            q.add( new TermQuery( new Term( key, value.toString() ) ), Occur.MUST );
            return q;
        }

        @Override
        public Query get( String key, Object value )
        {
            return new TermQuery( new Term( key, value.toString() ) );
        }

        @Override
        public void addToDocument( Document document, String key, Object value )
        {
            document.add( instantiateField( key, value, Index.NOT_ANALYZED ) );
        }
        
        @Override
        public void removeFromDocument( Document document, String key, Object value )
        {
            String stringValue = value.toString();
            Set<String> values = new HashSet<String>( Arrays.asList(
                    document.getValues( key ) ) );
            if ( !values.remove( stringValue ) )
            {
                return;
            }
            document.removeFields( key );
            for ( String existingValue : values )
            {
                addToDocument( document, key, existingValue );
            }
        }
        
        public TxData newTxData(LuceneIndex index)
        {
            return new ExactTxData( index );
        }
    };
    
    private static class FulltextType extends IndexType
    {
        FulltextType( Analyzer analyzer )
        {
            super( analyzer );
        }
        
        @Override
        public Query deletionQuery( long entityId, String key, Object value )
        {
            BooleanQuery q = new BooleanQuery();
            q.add( idTermQuery( entityId ), Occur.MUST );
            q.add( new TermQuery( new Term( exactKey( key ), value.toString() ) ), Occur.MUST );
            return q;
        }

        @Override
        public Query get( String key, Object value )
        {
            return new TermQuery( new Term( exactKey( key ), value.toString() ) );
        }

        private String exactKey( String key )
        {
            return key + "_e";
        }

        @Override
        public void addToDocument( Document document, String key, Object value )
        {
            document.add( new Field( exactKey( key ), value.toString(), Store.YES, Index.NOT_ANALYZED ) );
            document.add( instantiateField( key, value.toString(), Index.ANALYZED ) );
        }
        
        @Override
        public void removeFromDocument( Document document, String key, Object value )
        {
            String stringValue = value.toString();
            String exactKey = exactKey( key );
            Set<String> values = new HashSet<String>(
                    Arrays.asList( document.getValues( exactKey ) ) );
            if ( !values.remove( stringValue ) )
            {
                return;
            }
            document.removeFields( exactKey );
            document.removeFields( key );
            for ( String existingValue : values )
            {
                addToDocument( document, key, existingValue );
            }
        }
        
        @Override
        public TxData newTxData( LuceneIndex index )
        {
            return new FullTxData( index );
        }
    };
    
    final Analyzer analyzer;
    
    private IndexType( Analyzer analyzer )
    {
        this.analyzer = analyzer;
    }
    
    private static String configKey( String indexName, String property )
    {
        return property;
    }
    
    static IndexType getIndexType( IndexIdentifier identifier )
    {
        Map<String, String> config = identifier.config != null ? identifier.config :
                new HashMap<String, String>();
        String type = config.get( configKey( identifier.indexName, "type" ) );
        IndexType result = null;
        if ( type.equals( "exact" ) )
        {
            result = EXACT;
        }
        else if ( type.equals( "fulltext" ) )
        {
            result = new FulltextType( getAnalyzer( config, identifier.indexName ) );
        }
        else
        {
            throw new RuntimeException( "Unknown type '" + type + "' for index '" +
                    identifier.indexName + "'" );
        }
        return result;
    }
    
    private static Analyzer getAnalyzer( Map<String, String> config, String indexName )
    {
        String analyzerClass = config.get( configKey( indexName, "analyzer" ) );
        if ( analyzerClass != null )
        {
            try
            {
                return Class.forName( analyzerClass ).asSubclass( Analyzer.class ).newInstance();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
        
        String lowerCase = config.get( configKey( indexName, "to_lower_case" ) );
        if ( lowerCase == null || Boolean.parseBoolean( lowerCase ) )
        {
            return LuceneDataSource.LOWER_CASE_WHITESPACE_ANALYZER;
        }
        else
        {
            return LuceneDataSource.WHITESPACE_ANALYZER;
        }
    }

    abstract Query deletionQuery( long entityId, String key, Object value );
    
    abstract Query get( String key, Object value );
    
    abstract TxData newTxData( LuceneIndex index );
    
    Query query( String keyOrNull, Object value, QueryContext contextOrNull )
    {
        if ( value instanceof Query )
        {
            return (Query) value;
        }
        
        QueryParser parser = new QueryParser( Version.LUCENE_30, keyOrNull, analyzer );
        parser.setAllowLeadingWildcard( true );
        parser.setLowercaseExpandedTerms( false );
        if ( contextOrNull != null && contextOrNull.defaultOperator != null )
        {
            parser.setDefaultOperator( contextOrNull.defaultOperator );
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
    
    abstract void addToDocument( Document document, String key, Object value );
    
    Fieldable instantiateField( String key, Object value, Index analyzed )
    {
//        NumericField f = new NumericField( key );
//        f.setIntValue( ((Number) value).intValue() );
//        return f;
        
        Fieldable field = null;
        if ( value instanceof Number )
        {
            Number number = (Number) value;
            NumericField numberField = new NumericField( key, Store.YES, true );
            if ( value instanceof Long )
            {
                numberField.setLongValue( number.longValue() );
            }
            else if ( value instanceof Float )
            {
                numberField.setFloatValue( number.floatValue() );
            }
            else if ( value instanceof Double )
            {
                numberField.setDoubleValue( number.doubleValue() );
            }
            else
            {
                numberField.setIntValue( number.intValue() );
            }
            field = numberField;
        }
        else
        {
            field = new Field( key, value.toString(), Store.YES, analyzed );
        }
        return field;
    }
    
    abstract void removeFromDocument( Document document, String key, Object value );
    
    static Document newBaseDocument( long entityId )
    {
        Document doc = new Document();
        doc.add( new Field( LuceneIndex.KEY_DOC_ID, "" + entityId, Store.YES,
                Index.NOT_ANALYZED ) );
        return doc;
    }

    Term idTerm( long entityId )
    {
        return new Term( LuceneIndex.KEY_DOC_ID, "" + entityId );
    }
    
    Query idTermQuery( long entityId )
    {
        return new TermQuery( idTerm( entityId ) );
    }
}
