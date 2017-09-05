/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema.populator;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.UpdateMode;

/**
 * An {@link IndexUpdater} used while index population is in progress. Takes special care of node property additions
 * and changes applying them via {@link LuceneIndexWriter#updateDocument(Term, Document)} to make sure no duplicated
 * documents are inserted.
 */
public abstract class LuceneIndexPopulatingUpdater implements IndexUpdater
{
    private final LuceneIndexWriter writer;

    public LuceneIndexPopulatingUpdater( LuceneIndexWriter writer )
    {
        this.writer = writer;
    }

    @Override
    public void process( IndexEntryUpdate<?> update ) throws IOException, IndexEntryConflictException
    {
        long nodeId = update.getEntityId();

        switch ( update.updateMode() )
        {
        case ADDED:
            added( update );
            writer.updateDocument( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ),
                    LuceneDocumentStructure.documentRepresentingProperties( nodeId, update.values() ) );
            break;
        case CHANGED:
            changed( update );
            writer.updateDocument( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ),
                    LuceneDocumentStructure.documentRepresentingProperties( nodeId, update.values() ) );
            break;
        case REMOVED:
            removed( update );
            writer.deleteDocuments( LuceneDocumentStructure.newTermForChangeOrRemove( nodeId ) );
            break;
        default:
            throw new IllegalStateException( "Unknown update mode " + update.values() );
        }
    }

    /**
     * Method is invoked when {@link IndexEntryUpdate} with {@link UpdateMode#ADDED} is processed.
     *
     * @param update the update being processed.
     */
    protected abstract void added( IndexEntryUpdate<?> update );

    /**
     * Method is invoked when {@link IndexEntryUpdate} with {@link UpdateMode#CHANGED} is processed.
     *
     * @param update the update being processed.
     */
    protected abstract void changed( IndexEntryUpdate<?> update );

    /**
     * Method is invoked when {@link IndexEntryUpdate} with {@link UpdateMode#REMOVED} is processed.
     *
     * @param update the update being processed.
     */
    protected abstract void removed( IndexEntryUpdate<?> update );
}
