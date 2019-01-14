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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.LruCache;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.ExplicitIndexHits;
import org.neo4j.kernel.api.impl.index.collector.DocValuesCollector;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

class LuceneBatchInserterIndex implements BatchInserterIndex
{
    private final IndexIdentifier identifier;
    private final IndexType type;

    private IndexWriter writer;
    private SearcherManager searcherManager;
    private final boolean createdNow;
    private Map<String, LruCache<String, Collection<EntityId>>> cache;
    private int updateCount;
    private final int commitBatchSize = 500000;
    private final RelationshipLookup relationshipLookup;

    interface RelationshipLookup
    {
        EntityId lookup( long id );
    }

    LuceneBatchInserterIndex( File dbStoreDir,
            IndexIdentifier identifier, Map<String, String> config, RelationshipLookup relationshipLookup )
    {
        File storeDir = getStoreDir( dbStoreDir );
        this.createdNow = !LuceneDataSource.getFileDirectory( storeDir, identifier ).exists();
        this.identifier = identifier;
        this.type = IndexType.getIndexType( config );
        this.relationshipLookup = relationshipLookup;
        this.writer = instantiateWriter( storeDir );
        this.searcherManager = instantiateSearcherManager( writer );
    }

    @Override
    public void add( long id, Map<String, Object> properties )
    {
        try
        {
            Document document = IndexType.newDocument( entityId( id ) );
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
                String key = entry.getKey();
                Object value = entry.getValue();
                addSingleProperty( id, document, key, value );
            }
            writer.addDocument( document );
            if ( ++updateCount == commitBatchSize )
            {
                writer.commit();
                updateCount = 0;
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private EntityId entityId( long id )
    {
        if ( identifier.entityType == IndexEntityType.Node )
        {
            return new EntityId.IdData( id );
        }

        return relationshipLookup.lookup( id );
    }

    private void addSingleProperty( long entityId, Document document, String key, Object value )
    {
        for ( Object oneValue : IoPrimitiveUtils.asArray(value) )
        {
            boolean isValueContext = oneValue instanceof ValueContext;
            oneValue = isValueContext ? ((ValueContext) oneValue).getCorrectValue() : oneValue.toString();
            type.addToDocument( document, key, oneValue );
            if ( createdNow )
            {
                // If we know that the index was created this session
                // then we can go ahead and add stuff to the cache directly
                // when adding to the index.
                addToCache( entityId, key, oneValue );
            }
        }
    }

    private void addToCache( long entityId, String key, Object value )
    {
        if ( this.cache == null )
        {
            return;
        }

        String valueAsString = value.toString();
        LruCache<String, Collection<EntityId>> cache = this.cache.get( key );
        if ( cache != null )
        {
            Collection<EntityId> ids = cache.get( valueAsString );
            if ( ids == null )
            {
                ids = new HashSet<>();
                cache.put( valueAsString, ids );
            }
            ids.add( new EntityId.IdData( entityId ) );
        }
    }

    private void addToCache( Collection<EntityId> ids, String key, Object value )
    {
        if ( this.cache == null )
        {
            return;
        }

        String valueAsString = value.toString();
        LruCache<String, Collection<EntityId>> cache = this.cache.get( key );
        if ( cache != null )
        {
            cache.put( valueAsString, ids );
        }
    }

    private ExplicitIndexHits getFromCache( String key, Object value )
    {
        if ( this.cache == null )
        {
            return null;
        }

        String valueAsString = value.toString();
        LruCache<String, Collection<EntityId>> cache = this.cache.get( key );
        if ( cache != null )
        {
            Collection<EntityId> ids = cache.get( valueAsString );
            if ( ids != null )
            {
                return new ConstantScoreIterator( ids, Float.NaN );
            }
        }
        return null;
    }

    @Override
    public void updateOrAdd( long entityId, Map<String, Object> properties )
    {
        try
        {
            removeFromCache( entityId );
            writer.deleteDocuments( type.idTermQuery( entityId ) );
            add( entityId, properties );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void removeFromCache( long entityId ) throws IOException
    {
        IndexSearcher searcher = searcherManager.acquire();
        try
        {
            Query query = type.idTermQuery( entityId );
            TopDocs docs = searcher.search( query, 1 );
            if ( docs.totalHits > 0 )
            {
                Document document = searcher.doc( docs.scoreDocs[0].doc );
                for ( IndexableField field : document.getFields() )
                {
                    String key = field.name();
                    Object value = field.stringValue();
                    removeFromCache( entityId, key, value );
                }
            }
        }
        finally
        {
            searcherManager.release( searcher );
        }
    }

    private void removeFromCache( long entityId, String key, Object value )
    {
        if ( this.cache == null )
        {
            return;
        }

        String valueAsString = value.toString();
        LruCache<String, Collection<EntityId>> cache = this.cache.get( key );
        if ( cache != null )
        {
            Collection<EntityId> ids = cache.get( valueAsString );
            if ( ids != null )
            {
                ids.remove( new EntityId.IdData( entityId ) );
            }
        }
    }

    private IndexWriter instantiateWriter( File folder )
    {
        Directory dir = null;
        try
        {
            dir = LuceneDataSource.getDirectory( folder, identifier );
            IndexWriterConfig writerConfig = new IndexWriterConfig( type.analyzer );
            writerConfig.setRAMBufferSizeMB( determineGoodBufferSize( writerConfig.getRAMBufferSizeMB() ) );
            return new IndexWriter( dir, writerConfig );
        }
        catch ( IOException e )
        {
            IOUtils.closeAllSilently( dir );
            throw new RuntimeException( e );
        }
    }

    private double determineGoodBufferSize( double atLeast )
    {
        double heapHint = Runtime.getRuntime().maxMemory() / (1024 * 1024 * 14);
        double result = Math.max( atLeast, heapHint );
        return Math.min( result, 700 );
    }

    private static SearcherManager instantiateSearcherManager( IndexWriter writer )
    {
        try
        {
            return new SearcherManager( writer, true, new SearcherFactory() );
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
            if ( searcherManager != null )
            {
                this.searcherManager.close();
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            this.searcherManager = null;
        }
    }

    private void closeWriter()
    {
        try
        {
            if ( this.writer != null )
            {
                this.writer.close();
            }
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

    private IndexHits<Long> query( Query query, final String key, final Object value )
    {
        IndexSearcher searcher;
        try
        {
            searcher = searcherManager.acquire();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        try
        {
            DocValuesCollector collector = new DocValuesCollector( true );
            searcher.search( query, collector );
            IndexHits<Document> result = collector.getIndexHits( Sort.RELEVANCE );
            ExplicitIndexHits primitiveHits = null;
            if ( key == null || this.cache == null || !this.cache.containsKey( key ) )
            {
                primitiveHits = new DocToIdIterator( result, Collections.emptyList(), null,
                        PrimitiveLongCollections.emptySet() );
            }
            else
            {
                primitiveHits = new DocToIdIterator( result, Collections.emptyList(), null,
                        PrimitiveLongCollections.emptySet() )
                {
                    private final Collection<EntityId> ids = new ArrayList<>();

                    @Override
                    protected boolean fetchNext()
                    {
                        if ( super.fetchNext() )
                        {
                            ids.add( new EntityId.IdData( next ) );
                            return true;
                        }
                        addToCache( ids, key, value );
                        return false;
                    }
                };
            }
            return wrapIndexHits( primitiveHits );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            try
            {
                searcherManager.release( searcher );
            }
            catch ( IOException ignore )
            {
            }
        }
    }

    private IndexHits<Long> wrapIndexHits( final ExplicitIndexHits ids )
    {
        return new IndexHits<Long>()
        {
            @Override
            public boolean hasNext()
            {
                return ids.hasNext();
            }

            @Override
            public Long next()
            {
                return ids.next();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ResourceIterator<Long> iterator()
            {
                return this;
            }

            @Override
            public int size()
            {
                return ids.size();
            }

            @Override
            public void close()
            {
                ids.close();
            }

            @Override
            public Long getSingle()
            {
                try
                {
                    long singleId = PrimitiveLongCollections.single( ids, -1L );
                    return singleId == -1 ? null : singleId;
                }
                finally
                {
                    close();
                }
            }

            @Override
            public float currentScore()
            {
                return 0;
            }
        };
    }

    @Override
    public IndexHits<Long> get( String key, Object value )
    {
        ExplicitIndexHits cached = getFromCache( key, value );
        return cached != null ? wrapIndexHits( cached ) : query( type.get( key, value ), key, value );
    }

    @Override
    public IndexHits<Long> query( String key, Object queryOrQueryObject )
    {
        return query( type.query( key, queryOrQueryObject, null ), null, null );
    }

    @Override
    public IndexHits<Long> query( Object queryOrQueryObject )
    {
        return query( type.query( null, queryOrQueryObject, null ), null, null );
    }

    public void shutdown()
    {
        closeSearcher();
        closeWriter();
    }

    private File getStoreDir( File dbStoreDir )
    {
        File dir = new File( dbStoreDir, "index" );
        if ( !dir.exists() && !dir.mkdirs() )
        {
            throw new RuntimeException( "Unable to create directory path["
                    + dir.getAbsolutePath() + "] for Neo4j store." );
        }
        return dir;
    }

    @Override
    public void flush()
    {
        try
        {
            searcherManager.maybeRefreshBlocking();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void setCacheCapacity( String key, int size )
    {
        if ( this.cache == null )
        {
            this.cache = new HashMap<>();
        }
        LruCache<String, Collection<EntityId>> cache = this.cache.get( key );
        if ( cache != null )
        {
            cache.resize( size );
        }
        else
        {
            cache = new LruCache<>( "Batch inserter cache for " + key, size );
            this.cache.put( key, cache );
        }
    }
}
