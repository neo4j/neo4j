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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.DocumentLogic;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.WriterLogic;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

class LuceneIndexAccessor implements IndexAccessor
{
    private final IndexWriter writer;
    private final File dir;
    private final DocumentLogic documentLogic;
    private final RefreshableIndexSearcher refreshableSearcher;
    private final WriterLogic writerLogic;
    private final FileSystemAbstraction fileSystem;
    
    public LuceneIndexAccessor( IndexWriterFactory indexWriterFactory, FileSystemAbstraction fileSystem, File dir,
            DocumentLogic documentLogic, WriterLogic writerLogic ) throws IOException
    {
        this.fileSystem = fileSystem;
        this.dir = dir;
        this.documentLogic = documentLogic;
        this.writerLogic = writerLogic;
        this.writer = indexWriterFactory.create( dir );
        this.refreshableSearcher = new RefreshableIndexSearcher( writer );
    }

    @Override
    public void drop()
    {
        try
        {
            writerLogic.close( writer );
            fileSystem.deleteRecursively( dir );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void updateAndCommit( Iterable<NodePropertyUpdate> updates )
    {
        try
        {
            for ( NodePropertyUpdate update : updates )
            {
                switch ( update.getUpdateMode() )
                {
                case ADDED:
                    add( update.getNodeId(), update.getValueAfter() );
                    break;
                case CHANGED:
                    change( update.getNodeId(), update.getValueBefore(), update.getValueAfter() );
                    break;
                case REMOVED:
                    remove( update.getNodeId(), update.getValueBefore() );
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
            }
            
            // Call refresh here since we are guaranteed to be the only thread writing concurrently.
            refreshableSearcher.refresh();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void add( long nodeId, Object value ) throws IOException
    {
        writer.addDocument( documentLogic.newDocument( nodeId, value ) );
    }

    private void change( long nodeId, Object valueBefore, Object valueAfter ) throws IOException
    {
        writer.updateDocument( documentLogic.newQueryForChangeOrRemove( nodeId, valueBefore ),
                documentLogic.newDocument( nodeId, valueAfter ) );
    }
    
    private void remove( long nodeId, Object value ) throws IOException
    {
        writer.deleteDocuments( documentLogic.newQueryForChangeOrRemove( nodeId, value ) );
    }
    
    @Override
    public void force()
    {
        try
        {
            writerLogic.forceAndMarkAsOnline( writer );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void close()
    {
        try
        {
            writerLogic.close( writer );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public IndexReader newReader()
    {
        return new LuceneIndexAccessorReader( refreshableSearcher.getUpToDateSearcher(), documentLogic );
    }
}
