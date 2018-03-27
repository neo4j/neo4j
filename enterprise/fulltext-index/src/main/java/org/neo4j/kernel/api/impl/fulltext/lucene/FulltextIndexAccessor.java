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
package org.neo4j.kernel.api.impl.fulltext.lucene;

import java.io.IOException;

import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure.documentRepresentingProperties;
import static org.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure.newTermForChangeOrRemove;

public class FulltextIndexAccessor extends AbstractLuceneIndexAccessor<FulltextIndexReader,FulltextIndex,FulltextIndexDescriptor>
{
    public FulltextIndexAccessor( FulltextIndex luceneIndex, FulltextIndexDescriptor descriptor )
    {
        super( luceneIndex, descriptor );
    }

    @Override
    public FulltextIndexUpdater getIndexUpdater( IndexUpdateMode mode )
    {
        return new FulltextIndexUpdater( mode.requiresIdempotency(), mode.requiresRefresh() );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        return super.newAllEntriesReader( LuceneFulltextDocumentStructure::getNodeId );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor propertyAccessor )
    {
        //The fulltext index does not care about constraints.
    }

    private class FulltextIndexUpdater extends AbstractLuceneIndexUpdater
    {

        private FulltextIndexUpdater( boolean idempotent, boolean refresh )
        {
            super( idempotent, refresh );
        }

        protected void addIdempotent( long nodeId, Value[] values ) throws IOException
        {
            writer.updateDocument( newTermForChangeOrRemove( nodeId ), documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
        }

        protected void add( long nodeId, Value[] values ) throws IOException
        {
            writer.addDocument( documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
        }

        protected void change( long nodeId, Value[] values ) throws IOException
        {
            writer.updateDocument( newTermForChangeOrRemove( nodeId ), documentRepresentingProperties( nodeId, descriptor.propertyNames(), values ) );
        }

        protected void remove( long nodeId ) throws IOException
        {
            writer.deleteDocuments( newTermForChangeOrRemove( nodeId ) );
        }
    }
}
