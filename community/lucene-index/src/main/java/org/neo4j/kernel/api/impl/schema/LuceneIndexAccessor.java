/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

public class LuceneIndexAccessor extends AbstractLuceneIndexAccessor<IndexReader,SchemaIndex,IndexDescriptor>
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
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
            throws IndexEntryConflictException, IOException
    {
        luceneIndex.verifyUniqueness( propertyAccessor, descriptor.schema().getPropertyIds() );
    }

    private class LuceneSchemaIndexUpdater extends AbstractLuceneIndexUpdater
    {

        protected LuceneSchemaIndexUpdater( boolean idempotent, boolean refresh )
        {
            super( idempotent, refresh );
        }

        @Override
        protected void addIdempotent( long nodeId, Value[] values ) throws IOException
        {
            writer.updateDocument( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ),
                    LuceneDocumentStructure.documentRepresentingProperties( nodeId, values ) );
        }

        @Override
        protected void add( long nodeId, Value[] values ) throws IOException
        {
            writer.addDocument( LuceneDocumentStructure.documentRepresentingProperties( nodeId, values ) );
        }

        @Override
        protected void change( long nodeId, Value[] values ) throws IOException
        {
            writer.updateDocument( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ),
                    LuceneDocumentStructure.documentRepresentingProperties( nodeId, values ) );
        }

        @Override
        protected void remove( long nodeId ) throws IOException
        {
            writer.deleteDocuments( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ) );
        }
    }
}
