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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.DocumentLogic;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.WriterLogic;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

class LuceneIndexPopulator implements IndexPopulator
{
    private org.apache.lucene.index.IndexWriter writer;
    private final List<NodePropertyUpdate> updates = new ArrayList<NodePropertyUpdate>();
    private final int queueThreshold;
    private final LuceneIndexWriterFactory indexWriterFactory;
    private final DocumentLogic documentLogic;
    private final WriterLogic writerLogic;
    private final DirectoryFactory dirFactory;
    private final File dirFile;

    private Directory directory;

    LuceneIndexPopulator( LuceneIndexWriterFactory indexWriterFactory, DirectoryFactory dirFactory, File dirFile,
            int queueThreshold, DocumentLogic documentLogic, WriterLogic writerLogic )
    {
        this.indexWriterFactory = indexWriterFactory;
        this.queueThreshold = queueThreshold;
        this.documentLogic = documentLogic;
        this.writerLogic = writerLogic;
        this.dirFactory = dirFactory;
        this.dirFile = dirFile;
    }


    @Override
    public void create()
    {
        try
        {
            this.directory = dirFactory.open( dirFile );
            DirectorySupport.deleteDirectoryContents( directory );
            writer = indexWriterFactory.create( directory );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void drop()
    {
        try
        {
            writerLogic.close( writer );
            DirectorySupport.deleteDirectoryContents( directory );
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
            writer.addDocument( documentLogic.newDocument( nodeId, propertyValue ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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
        for ( NodePropertyUpdate update : this.updates )
        {
            long nodeId = update.getNodeId();
            switch ( update.getUpdateMode() )
            {
            case ADDED:
                writer.addDocument( documentLogic.newDocument( nodeId, update.getValueAfter() ) );
                break;
            case CHANGED:
                writer.updateDocument( documentLogic.newQueryForChangeOrRemove( nodeId, update.getValueBefore() ),
                        documentLogic.newDocument( nodeId, update.getValueAfter() ) );
                break;
            case REMOVED:
                writer.deleteDocuments( documentLogic.newQueryForChangeOrRemove( nodeId, update.getValueBefore() ) );
                break;
            }
        }
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        try
        {
            if ( populationCompletedSuccessfully )
            {
                applyQueuedUpdates();
                writerLogic.forceAndMarkAsOnline( writer );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            try
            {
                writerLogic.close( writer );
                directory.close();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
