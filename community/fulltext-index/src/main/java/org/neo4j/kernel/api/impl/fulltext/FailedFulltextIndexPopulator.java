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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Value;

class FailedFulltextIndexPopulator extends IndexPopulator.Adapter
{
    private final IndexDescriptor index;
    private final DatabaseIndex<FulltextIndexReader> fulltextIndex;
    private final Exception exception;

    FailedFulltextIndexPopulator( IndexDescriptor index, DatabaseIndex<FulltextIndexReader> fulltextIndex, Exception exception )
    {
        this.index = index;
        this.fulltextIndex = fulltextIndex;
        this.exception = exception;
    }

    @Override
    public void create()
    {
        // We don't fail in create(), because if this population job is running as part of a multiple-index-population job,
        // then throwing from create would fail the entire index population cohort. And we only want to fail this one index.
    }

    @Override
    public void drop()
    {
        fulltextIndex.drop();
    }

    @Override
    public void add( Collection<? extends IndexEntryUpdate<?>> updates )
    {
        throw failedException();
    }

    private IllegalStateException failedException()
    {
        return new IllegalStateException( "Failed to create index populator.", exception );
    }

    @Override
    public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
    {
        return new IndexUpdater()
        {
            @Override
            public void process( IndexEntryUpdate<?> update )
            {
                throw failedException();
            }

            @Override
            public void close()
            {
            }
        };
    }

    @Override
    public void markAsFailed( String failure )
    {
        try
        {
            fulltextIndex.markAsFailed( failure );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        return index.getIndexConfig().asMap();
    }
}
