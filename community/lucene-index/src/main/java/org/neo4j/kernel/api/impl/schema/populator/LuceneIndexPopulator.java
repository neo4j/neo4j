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
package org.neo4j.kernel.api.impl.schema.populator;

import org.apache.lucene.document.Document;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;

/**
 * An {@link IndexPopulator} used to create, populate and mark as online a Lucene schema index.
 */
public abstract class LuceneIndexPopulator<INDEX extends DatabaseIndex<?>> implements IndexPopulator
{
    protected INDEX luceneIndex;
    protected LuceneIndexWriter writer;

    protected LuceneIndexPopulator( INDEX luceneIndex )
    {
        this.luceneIndex = luceneIndex;
    }

    @Override
    public void create()
    {
        try
        {
            luceneIndex.create();
            luceneIndex.open();
            writer = luceneIndex.getIndexWriter();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void drop()
    {
        luceneIndex.drop();
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates )
    {
        assert updatesForCorrectIndex( updates );

        try
        {
            // Lucene documents stored in a ThreadLocal and reused so we can't create an eager collection of documents here
            // That is why we create a lazy Iterator and then Iterable
            writer.addDocuments( updates.size(), () -> updates.stream()
                    .map( LuceneIndexPopulator::updateAsDocument )
                    .iterator() );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void close( boolean populationCompletedSuccessfully )
    {
        try
        {
            if ( populationCompletedSuccessfully )
            {
                luceneIndex.markAsOnline();
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            IOUtils.closeAllSilently( luceneIndex );
        }
    }

    @Override
    public void markAsFailed( String failure )
    {
        try
        {
            luceneIndex.markAsFailed( failure );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private boolean updatesForCorrectIndex( Collection<? extends IndexEntryUpdate<?>> updates )
    {
        for ( IndexEntryUpdate<?> update : updates )
        {
            if ( !update.indexKey().schema().equals( luceneIndex.getDescriptor().schema() ) )
            {
                return false;
            }
        }
        return true;
    }

    private static Document updateAsDocument( IndexEntryUpdate<?> update )
    {
        return LuceneDocumentStructure.documentRepresentingProperties( update.getEntityId(), update.values() );
    }
}
