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

import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.util.FailureStorage;

class NonUniqueLuceneIndexPopulator extends LuceneIndexPopulator
{
    static final int DEFAULT_QUEUE_THRESHOLD = 10000;
    private final int queueThreshold;
    private final List<NodePropertyUpdate> updates = new ArrayList<NodePropertyUpdate>();

    NonUniqueLuceneIndexPopulator( int queueThreshold, LuceneDocumentStructure documentStructure,
                                   LuceneIndexWriterFactory indexWriterFactory,
                                   IndexWriterStatus writerStatus, DirectoryFactory dirFactory, File dirFile,
                                   FailureStorage failureStorage, long indexId )
    {
        super( documentStructure, indexWriterFactory, writerStatus, dirFactory, dirFile, failureStorage, indexId );
        this.queueThreshold = queueThreshold;
    }

    @Override
    public void add( long nodeId, Object propertyValue ) throws IOException
    {
        writer.addDocument( documentStructure.newDocument( nodeId, propertyValue ) );
    }

    @Override
    public void update( Iterable<NodePropertyUpdate> updates ) throws IOException
    {
        for ( NodePropertyUpdate update : updates )
        {
            this.updates.add( update );
        }

        if ( this.updates.size() > queueThreshold )
        {
            flush();
            this.updates.clear();
        }
    }

    @Override
    protected void flush() throws IOException
    {
        for ( NodePropertyUpdate update : this.updates )
        {
            long nodeId = update.getNodeId();
            switch ( update.getUpdateMode() )
            {
            case ADDED:
                writer.addDocument( documentStructure.newDocument( nodeId, update.getValueAfter() ) );
                break;
            case CHANGED:
                writer.updateDocument( documentStructure.newQueryForChangeOrRemove( nodeId ),
                                       documentStructure.newDocument( nodeId, update.getValueAfter() ) );
                break;
            case REMOVED:
                writer.deleteDocuments( documentStructure.newQueryForChangeOrRemove( nodeId ) );
                break;
            }
        }
    }
}
