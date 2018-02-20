/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure;
import org.neo4j.kernel.api.impl.fulltext.lucene.ReadOnlyFulltext;
import org.neo4j.kernel.api.impl.fulltext.lucene.WritableFulltext;
import org.neo4j.kernel.api.impl.schema.reader.LuceneAllEntriesIndexAccessorReader;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

public class FulltextIndexAccessor implements IndexAccessor
{
    private WritableFulltext luceneFulltext;
    private FulltextIndexDescriptor descriptor;

    public FulltextIndexAccessor( WritableFulltext luceneFulltext, FulltextIndexDescriptor descriptor )
    {
        this.luceneFulltext = luceneFulltext;
        this.descriptor = descriptor;
    }

    @Override
    public void drop() throws IOException
    {
        luceneFulltext.drop();
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        if ( luceneFulltext.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create updater for read only index." );
        }
        return new FulltextIndexUpdater( mode.requiresIdempotency(), mode.requiresRefresh() );
    }

    @Override
    public void force() throws IOException
    {
        luceneFulltext.markAsOnline();
        luceneFulltext.maybeRefreshBlocking();
    }

    @Override
    public void refresh() throws IOException
    {
        luceneFulltext.maybeRefreshBlocking();
    }

    @Override
    public void close() throws IOException
    {
        luceneFulltext.close();
    }

    @Override
    public IndexReader newReader()
    {
        System.out.println( "Tried to get reader" );
        return IndexReader.EMPTY;
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return new LuceneAllEntriesIndexAccessorReader( luceneFulltext.allDocumentsReader() );
    }

    @Override
    public ResourceIterator<File> snapshotFiles() throws IOException
    {
        throw new UnsupportedOperationException( "Not implemented" );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor ) throws IndexEntryConflictException, IOException
    {
        //Sure whatever
    }

    public PrimitiveLongIterator query( String query ) throws IOException
    {
        try ( ReadOnlyFulltext indexReader = luceneFulltext.getIndexReader() )
        {
            return indexReader.query( query );
        }
    }

    private class FulltextIndexUpdater implements IndexUpdater
    {
        private final boolean idempotent;
        private final boolean refresh;

        private boolean hasChanges;

        private FulltextIndexUpdater( boolean idempotent, boolean refresh )
        {
            this.idempotent = idempotent;
            this.refresh = refresh;
        }

        @Override
        public void process( IndexEntryUpdate<?> update ) throws IOException
        {
            // we do not support adding partial entries
            assert update.indexKey().schema().equals( descriptor.schema() );

            switch ( update.updateMode() )
            {
            case ADDED:
                if ( idempotent )
                {
                    addIdempotent( update.getEntityId(), update.values() );
                }
                else
                {
                    add( update.getEntityId(), update.values() );
                }
                break;
            case CHANGED:
                change( update.getEntityId(), update.values() );
                break;
            case REMOVED:
                remove( update.getEntityId() );
                break;
            default:
                throw new UnsupportedOperationException();
            }
            hasChanges = true;
        }

        @Override
        public void close() throws IOException, IndexEntryConflictException
        {
            if ( hasChanges && refresh )
            {
                luceneFulltext.maybeRefreshBlocking();
            }
        }

        private void addIdempotent( long nodeId, Value[] values ) throws IOException
        {
            luceneFulltext.getIndexWriter().updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( nodeId ),
                    LuceneFulltextDocumentStructure.documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
        }

        private void add( long nodeId, Value[] values ) throws IOException
        {
            luceneFulltext.getIndexWriter().addDocument(
                    LuceneFulltextDocumentStructure.documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
        }

        private void change( long nodeId, Value[] values ) throws IOException
        {
            luceneFulltext.getIndexWriter().updateDocument( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( nodeId ),
                    LuceneFulltextDocumentStructure.documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
        }

        protected void remove( long nodeId ) throws IOException
        {
            luceneFulltext.getIndexWriter().deleteDocuments( LuceneFulltextDocumentStructure.newTermForChangeOrRemove( nodeId ) );
        }
    }
}
