/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.apache.lucene.document.Field.Index.NOT_ANALYZED;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.impl.lucene.IndexType.instantiateField;
import static org.neo4j.index.impl.lucene.IndexType.newBaseDocument;
import static org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.KEY_STATUS;
import static org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.ONLINE;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Version;
import org.neo4j.index.impl.lucene.IndexType;
import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.kernel.impl.api.index.IndexPopulator;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.util.FileUtils;

class LuceneIndexPopulator implements IndexPopulator
{
    static final String SINGLE_KEY = "key";
    private final File dir;
    private final DirectoryFactory factory;
    private org.apache.lucene.index.IndexWriter writer;
    private IndexReader globalReader;
    private final List<NodePropertyUpdate> updates = new ArrayList<NodePropertyUpdate>();
    private final int queueThreshold;

    LuceneIndexPopulator( File dir, DirectoryFactory factory, int queueThreshold )
    {
        this.dir = dir;
        this.factory = factory;
        this.queueThreshold = queueThreshold;
    }

    @Override
    public void createIndex()
    {
        deleteDirectory();
        
        IndexWriterConfig writerConfig = new IndexWriterConfig( Version.LUCENE_35, LuceneDataSource.KEYWORD_ANALYZER );
        writerConfig.setMaxBufferedDocs( 100000 ); // TODO figure out depending on environment?
        try
        {
            writer = new IndexWriter( factory.open( dir ), writerConfig );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void deleteDirectory()
    {
        try
        {
            FileUtils.deleteRecursively( dir );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void dropIndex()
    {
        try
        {
            writer.close( true );
            FileUtils.deleteRecursively( dir );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void add( long nodeId, Object propertyValue )
    {
        try
        {
            writer.addDocument( newDocument( nodeId, propertyValue ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Document newDocument( long nodeId, Object propertyValue )
    {
        Document document = newBaseDocument( nodeId );
        addToDocument( document, propertyValue );
        return document;
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates )
    {
        for ( NodePropertyUpdate update : updates )
            this.updates.add( update );
        
        if ( this.updates.size() > queueThreshold )
        {
            try
            {
                applyQueuedUpdates();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
            this.updates.clear();
        }
    }

    private void applyQueuedUpdates() throws IOException
    {
        IndexReader reader = upToDateReader();
        IndexSearcher searcher = new IndexSearcher( reader );
        try
        {
            for ( NodePropertyUpdate update : this.updates )
            {
                switch ( update.getUpdateMode() )
                {
                case ADDED: addFromUpdate( reader, update.getNodeId(), update.getValueAfter() ); break;
                case CHANGED: changeFromUpdate( reader, update.getNodeId(), update.getValueBefore(), update.getValueAfter() ); break;
                case REMOVED: removeFromUpdate( reader, update.getNodeId(), update.getValueBefore() ); break;
                }
            }
        }
        finally
        {
            searcher.close();
        }
    }

    private void addFromUpdate( IndexReader reader, long nodeId, Object value ) throws IOException
    {
        writer.addDocument( newDocument( nodeId, value ) );
    }
    
    private void changeFromUpdate( IndexReader reader, long nodeId, Object valueBefore, Object valueAfter ) throws IOException
    {
        writer.updateDocument( nodeTerm( nodeId ), newDocument( nodeId, valueAfter ) );
    }
    
    private void removeFromUpdate( IndexReader reader, long nodeId, Object valueBefore ) throws IOException
    {
        writer.deleteDocuments( nodeTerm( nodeId ) );
    }

    private IndexReader upToDateReader() throws IOException
    {
        if ( globalReader == null )
            // This is the first time we read something, just open it
            return (globalReader = IndexReader.open( writer, false ));
        
        IndexReader refreshedReader = IndexReader.openIfChanged( globalReader, writer, false );
        if ( refreshedReader != null )
        {   // Something changed, update our reader reference
            globalReader.close();
            return (globalReader = refreshedReader);
        }
        
        // Nothing was changed, return the previous reader reference
        return globalReader;
    }

    @Override
    public void populationCompleted()
    {
        try
        {
            applyQueuedUpdates();
            writer.commit( stringMap( KEY_STATUS, ONLINE ) );
            writer.close( true );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Term nodeTerm( long nodeId )
    {
        return IndexType.EXACT.idTerm( nodeId );
    }

    private void addToDocument( Document document, Object value )
    {
        document.add( instantiateField( SINGLE_KEY, value, NOT_ANALYZED ) );
    }
}
