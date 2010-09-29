package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.neo4j.helpers.Pair;

class FullTxData extends TxData
{
    private Directory directory;
    private IndexWriter writer;
    private IndexSearcher searcher;
    private BooleanQuery extraQueries;
    
    FullTxData( LuceneIndex index )
    {
        super( index );
    }
    
    TxData add( Object entityId, String key, Object value )
    {
        try
        {
            ensureLuceneDataInstantiated();
            long id = entityId instanceof Long ? (Long) entityId : ((RelationshipId)entityId).id;
            Document document = LuceneDataSource.findDocument( index.type, searcher(), id );
            if ( document != null )
            {
                index.type.addToDocument( document, key, value );
                writer.updateDocument( index.type.idTerm( id ), document );
            }
            else
            {
                document = index.identifier.entityType.newDocument( entityId );
                index.type.addToDocument( document, key, value );
                writer.addDocument( document );
            }
            invalidateSearcher();
            return this;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    @Override
    TxData add( Query query )
    {
        if ( this.extraQueries == null )
        {
            this.extraQueries = new BooleanQuery();
        }
        this.extraQueries.add( query, Occur.SHOULD );
        return this;
    }
    
    private void ensureLuceneDataInstantiated()
    {
        if ( this.directory == null )
        {
            try
            {
                this.directory = new RAMDirectory();
                this.writer = new IndexWriter( directory, index.type.analyzer,
                        MaxFieldLength.UNLIMITED );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    TxData remove( Object entityId, String key, Object value )
    {
        try
        {
            ensureLuceneDataInstantiated();
            long id = entityId instanceof Long ? (Long) entityId : ((RelationshipId)entityId).id;
            Document document = LuceneDataSource.findDocument( index.type, searcher(), id );
            if ( document != null )
            {
                index.type.removeFromDocument( document, key, value );
                if ( LuceneDataSource.documentIsEmpty( document ) )
                {
                    writer.deleteDocuments( index.type.idTerm( id ) );
                }
                else
                {
                    writer.updateDocument( index.type.idTerm( id ), document );
                }
            }
            invalidateSearcher();
            return this;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    TxData remove( Query query )
    {
        ensureLuceneDataInstantiated();
        LuceneDataSource.remove( writer, query );
        invalidateSearcher();
        return this;
    }
    
    Pair<Collection<Long>, TxData> query( Query query, QueryContext contextOrNull )
    {
        return internalQuery( query, contextOrNull );
    }
    
    private Pair<Collection<Long>, TxData> internalQuery( Query query, QueryContext contextOrNull )
    {
        if ( this.directory == null )
        {
            return new Pair<Collection<Long>, TxData>( Collections.<Long>emptySet(), this );
        }
        
        try
        {
            Sort sorting = contextOrNull != null ? contextOrNull.sorting : null;
            Hits hits = new Hits( searcher(), query, null, sorting );
            Collection<Long> result = new ArrayList<Long>();
            for ( int i = 0; i < hits.length(); i++ )
            {
                result.add( Long.parseLong( hits.doc( i ).getField(
                    LuceneIndex.KEY_DOC_ID ).stringValue() ) );
            }
            return new Pair<Collection<Long>, TxData>( result, this );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    void close()
    {
        safeClose( this.writer );
        invalidateSearcher();
    }

    private void invalidateSearcher()
    {
        safeClose( this.searcher );
        this.searcher = null;
    }
    
    private IndexSearcher searcher()
    {
        try
        {
            if ( this.searcher == null )
            {
                this.writer.commit();
                this.searcher = new IndexSearcher( directory, true );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return this.searcher;
    }
    
    private static void safeClose( Object object )
    {
        if ( object == null )
        {
            return;
        }
        
        try
        {
            if ( object instanceof IndexWriter )
            {
                ( ( IndexWriter ) object ).close();
            }
            else if ( object instanceof IndexSearcher )
            {
                ( ( IndexSearcher ) object ).close();
            }
        }
        catch ( IOException e )
        {
            // Ok
        }
    }

    @Override
    Pair<Collection<Long>, TxData> get( String key, Object value )
    {
        return internalQuery( this.index.type.get( key, value ), null );
    }
    
    @Override
    Query getExtraQuery()
    {
        return this.extraQueries;
    }
    
    @Override
    TxData clear()
    {
        try
        {
            if ( writer != null )
            {
                writer.deleteAll();
            }
            invalidateSearcher();
            this.extraQueries = null;
            return this;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
