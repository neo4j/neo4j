/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.index.impl.PrimitiveUtils;
import org.neo4j.index.impl.IndexHitsImpl;
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
        try
        {
            Document document = identifier.entityType.newDocument( entityId );
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
                String key = entry.getKey();
                for ( Object value : PrimitiveUtils.asArray( entry.getValue() ) )
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
    
    public void updateOrAdd( long entityId, Map<String, Object> properties )
    {
        try
        {
            writer().deleteDocuments( type.idTermQuery( entityId ) );
            add( entityId, properties );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
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
            Hits hits = new Hits( searcher(), query, null );
            SearchResult result = new SearchResult( new HitsIterator( hits ), hits.length() );
            DocToIdIterator itr = new DocToIdIterator( result, null, null );
            return new IndexHitsImpl<Long>( FilteringIterator.noDuplicates( itr ), result.size );
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
        return query( type.query( key, queryOrQueryObject, null ) );
    }

    public IndexHits<Long> query( Object queryOrQueryObject )
    {
        return query( type.query( null, queryOrQueryObject, null ) );
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
