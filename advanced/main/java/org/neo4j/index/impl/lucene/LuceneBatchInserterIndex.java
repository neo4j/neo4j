package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Map;

import org.apache.lucene.AllDocs;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.neo4j.commons.iterator.FilteringIterator;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.impl.SimpleIndexHits;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

class LuceneBatchInserterIndex implements BatchInserterIndex
{
    private final String storeDir;
    private final IndexIdentifier identifier;
    private final IndexType type;
    
    private IndexWriter writer;
    private boolean writerModified;
    private IndexSearcher searcher;

    LuceneBatchInserterIndex( LuceneBatchInserterIndexProvider provider,
            BatchInserter inserter, IndexIdentifier identifier )
    {
        String dbStoreDir = ((BatchInserterImpl) inserter).getStore();
        this.storeDir = LuceneDataSource.getStoreDir( dbStoreDir );
        this.identifier = identifier;
        this.type = provider.typeCache.getIndexType( identifier );
    }
    
    public void add( long entityId, Map<String, Object> properties )
    {
        put( type.newDocument( entityId ), properties );
    }
    
    public void updateOrAdd( long entityId, Map<String, Object> properties )
    {
        Document document = LuceneDataSource.findDocument( type, searcher(), entityId );
        put( document != null ? document : type.newDocument( entityId ), properties );
    }
    
    private void put( Document document, Map<String, Object> properties )
    {
        try
        {
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
                String key = entry.getKey();
                for ( Object value : asArray( entry.getValue() ) )
                {
                    type.addToDocument( document, key, value );
                }
            }
            writer().addDocument( document );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private static Object[] asArray( Object propertyValue )
    {
        if ( propertyValue.getClass().isArray() )
        {
            int length = Array.getLength( propertyValue );
            Object[] result = new Object[ length ];
            for ( int i = 0; i < length; i++ )
            {
                result[ i ] = Array.get( propertyValue, i );
            }
            return result;
        }
        else
        {
            return new Object[] { propertyValue };
        }
    }

    private IndexWriter writer()
    {
        if ( this.writer != null )
        {
            return this.writer;
        }
        try
        {
            this.writer = new IndexWriter( LuceneDataSource.getDirectory( storeDir, identifier ),
                    type.analyzer, MaxFieldLength.UNLIMITED );
            return this.writer;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private void closeSearcher()
    {
        try
        {
            LuceneUtil.close( this.searcher );
        }
        finally
        {
            this.searcher = null;
        }
    }

    private IndexSearcher searcher()
    {
        IndexSearcher result = this.searcher;
        try
        {
            if ( result == null || writerModified )
            {
                if ( result != null )
                {
                    result.getIndexReader().close();
                    result.close();
                }
                IndexReader newReader = writer().getReader();
                result = new IndexSearcher( newReader );
                writerModified = false;
            }
            return result;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            this.searcher = result;
        }
    }
    
    private void closeWriter()
    {
        try
        {
            if ( this.writer != null )
            {
                this.writer.optimize( true );
            }
            LuceneUtil.close( this.writer );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            this.writer = null;
        }
    }

    private IndexHits<Long> query( Query query )
    {
        try
        {
            AllDocs hits = new AllDocs( searcher(), query, null );
            SearchResult result = new SearchResult( new HitsIterator( hits ), hits.length() );
            DocToIdIterator itr = new DocToIdIterator( result, null, null );
            return new SimpleIndexHits<Long>( FilteringIterator.noDuplicates( itr ), result.size );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public IndexHits<Long> get( String key, Object value )
    {
        return query( type.get( key, value ) );
    }

    public IndexHits<Long> query( String key, Object queryOrQueryObject )
    {
        return query( type.query( key, queryOrQueryObject ) );
    }

    public IndexHits<Long> query( Object queryOrQueryObject )
    {
        return query( type.query( null, queryOrQueryObject ) );
    }

    public void shutdown()
    {
        closeWriter();
        closeSearcher();
    }
    
    public void flush()
    {
        writerModified = true;
    }
}
