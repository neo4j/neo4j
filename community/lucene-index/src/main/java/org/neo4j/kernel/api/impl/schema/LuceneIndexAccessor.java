/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.LuceneIndexValueValidator;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Value;

public class LuceneIndexAccessor extends AbstractLuceneIndexAccessor<IndexReader,SchemaIndex>
{
    private final LuceneIndexValueValidator valueValidator;

    public LuceneIndexAccessor( SchemaIndex luceneIndex, IndexDescriptor descriptor, TokenNameLookup tokenNameLookup )
    {
        super( luceneIndex, descriptor );
        this.valueValidator = new LuceneIndexValueValidator( descriptor, tokenNameLookup );
    }

    @Override
    protected IndexUpdater getIndexUpdater( IndexUpdateMode mode )
    {
        return new LuceneSchemaIndexUpdater( mode.requiresIdempotency(), mode.requiresRefresh() );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader( long fromIdInclusive, long toIdExclusive, PageCursorTracer cursorTracer )
    {
        return super.newAllEntriesReader( LuceneDocumentStructure::getNodeId, fromIdInclusive, toIdExclusive );
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor )
            throws IndexEntryConflictException
    {
        try
        {
            luceneIndex.verifyUniqueness( nodePropertyAccessor, descriptor.schema().getPropertyIds() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void validateBeforeCommit( long entityId, Value[] tuple )
    {
        valueValidator.validate( entityId, tuple );
    }

    private class LuceneSchemaIndexUpdater extends AbstractLuceneIndexUpdater
    {

        LuceneSchemaIndexUpdater( boolean idempotent, boolean refresh )
        {
            super( idempotent, refresh );
        }

        @Override
        protected void addIdempotent( long nodeId, Value[] values )
        {
            try
            {
                writer.updateDocument( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ),
                        LuceneDocumentStructure.documentRepresentingProperties( nodeId, values ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        protected void add( long nodeId, Value[] values )
        {
            try
            {
                writer.addDocument( LuceneDocumentStructure.documentRepresentingProperties( nodeId, values ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        protected void change( long nodeId, Value[] values )
        {
            try
            {
                writer.updateDocument( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ),
                        LuceneDocumentStructure.documentRepresentingProperties( nodeId, values ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        protected void remove( long nodeId )
        {
            try
            {
                writer.deleteDocuments( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ) );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }
    }
}
