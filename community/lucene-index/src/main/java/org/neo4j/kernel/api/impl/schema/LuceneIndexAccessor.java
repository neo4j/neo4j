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
package org.neo4j.kernel.api.impl.schema;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.LuceneIndexValueValidator;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

public class LuceneIndexAccessor extends AbstractLuceneIndexAccessor<IndexReader,SchemaIndex>
{

    public LuceneIndexAccessor( SchemaIndex luceneIndex, IndexDescriptor descriptor )
    {
        super( luceneIndex, descriptor );
    }

    @Override
    protected IndexUpdater getIndexUpdater( IndexUpdateMode mode )
    {
        return new LuceneSchemaIndexUpdater( mode.requiresIdempotency(), mode.requiresRefresh() );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return super.newAllEntriesReader( LuceneDocumentStructure::getNodeId );
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
    public void validateBeforeCommit( Value[] tuple )
    {
        // In Lucene all values in a tuple (composite index) will be placed in a separate field, so validate their fields individually.
        for ( Value value : tuple )
        {
            LuceneIndexValueValidator.INSTANCE.validate( value );
        }
    }

    private class LuceneSchemaIndexUpdater extends AbstractLuceneIndexUpdater
    {

        protected LuceneSchemaIndexUpdater( boolean idempotent, boolean refresh )
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
