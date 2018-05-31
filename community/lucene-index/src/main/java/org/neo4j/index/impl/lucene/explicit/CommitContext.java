/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.impl.index.IndexCommand;

/**
 * This presents a context for each {@link IndexCommand} when they are
 * committing its data.
 */
class CommitContext implements Closeable
{
    final LuceneDataSource dataSource;
    final IndexIdentifier identifier;
    final IndexType indexType;
    final MutableLongObjectMap<DocumentContext> documents = new LongObjectHashMap<>();
    final boolean recovery;

    IndexReference searcher;
    IndexWriter writer;

    CommitContext( LuceneDataSource dataSource, IndexIdentifier identifier, IndexType indexType, boolean isRecovery )
    {
        this.dataSource = dataSource;
        this.identifier = identifier;
        this.indexType = indexType;
        this.recovery = isRecovery;
    }

    void ensureWriterInstantiated() throws ExplicitIndexNotFoundKernelException
    {
        if ( searcher == null )
        {
            searcher = dataSource.getIndexSearcher( identifier );
            writer = searcher.getWriter();
        }
    }

    DocumentContext getDocument( EntityId entityId, boolean allowCreate )
    {
        long id = entityId.id();
        DocumentContext context = documents.get( id );
        if ( context != null )
        {
            return context;
        }

        Document document = LuceneDataSource.findDocument( indexType, searcher.getSearcher(), id );
        if ( document != null )
        {
            context = new DocumentContext( document, true, id );
            documents.put( id, context );
        }
        else if ( allowCreate )
        {
            context = new DocumentContext( IndexType.newDocument( entityId ), false, id );
            documents.put( id, context );
        }
        return context;
    }

    private void applyDocuments( IndexWriter writer, IndexType type, LongObjectMap<DocumentContext> documents ) throws IOException
    {
        for ( DocumentContext context : documents )
        {
            if ( context.exists )
            {
                if ( LuceneDataSource.documentIsEmpty( context.document ) )
                {
                    writer.deleteDocuments( type.idTerm( context.entityId ) );
                }
                else
                {
                    writer.updateDocument( type.idTerm( context.entityId ), context.document );
                }
            }
            else
            {
                writer.addDocument( context.document );
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        applyDocuments( writer, indexType, documents );
        if ( writer != null )
        {
            dataSource.invalidateIndexSearcher( identifier );
        }
        if ( searcher != null )
        {
            searcher.close();
        }
    }

    static class DocumentContext
    {
        final Document document;
        final boolean exists;
        final long entityId;

        DocumentContext( Document document, boolean exists, long entityId )
        {
            this.document = document;
            this.exists = exists;
            this.entityId = entityId;
        }

        @Override
        public String toString()
        {
            return "DocumentContext[document=" + document + ", exists=" + exists + ", entityId=" + entityId + "]";
        }
    }
}
