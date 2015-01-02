/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.neo4j.index.impl.lucene.LuceneTransaction.CommandList;

/**
 * This presents a context for each {@link LuceneCommand} when they are
 * committing its data.
 */
class CommitContext
{
    final LuceneDataSource dataSource;
    final IndexIdentifier identifier;
    final IndexType indexType;
    final Map<Long, DocumentContext> documents = new HashMap<Long, DocumentContext>();
    final CommandList commandList;
    final boolean recovery;

    IndexReference searcher;
    IndexWriter writer;

    CommitContext( LuceneDataSource dataSource, IndexIdentifier identifier, IndexType indexType, CommandList commandList )
    {
        this.dataSource = dataSource;
        this.identifier = identifier;
        this.indexType = indexType;
        this.commandList = commandList;
        this.recovery = commandList.isRecovery();
    }

    void ensureWriterInstantiated()
    {
        if ( searcher == null )
        {
            searcher = dataSource.getIndexSearcher( identifier );
            writer = searcher.getWriter();
        }
    }

    DocumentContext getDocument( Object entityId, boolean allowCreate )
    {
        long id = entityId instanceof Long ? (Long) entityId : ( (RelationshipId) entityId ).id;
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
            context = new DocumentContext( identifier.entityType.newDocument( entityId ), false, id );
            documents.put( id, context );
        }
        return context;
    }

    public void close()
    {
        if ( searcher != null )
            searcher.close();
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
    }
}
