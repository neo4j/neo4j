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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.values.storable.Value;

/**
 * A {@link LuceneIndexPopulatingUpdater} used for unique Lucene schema indexes.
 * Verifies uniqueness of added and changed values when closed using
 * {@link SchemaIndex#verifyUniqueness(PropertyAccessor, int[], List)} method.
 */
public class UniqueLuceneIndexPopulatingUpdater extends LuceneIndexPopulatingUpdater
{
    private final int[] propertyKeyIds;
    private final SchemaIndex luceneIndex;
    private final PropertyAccessor propertyAccessor;
    private final UniqueIndexSampler sampler;

    private final List<Value[]> updatedValueTuples = new ArrayList<>();

    public UniqueLuceneIndexPopulatingUpdater( LuceneIndexWriter writer, int[] propertyKeyIds,
            SchemaIndex luceneIndex, PropertyAccessor propertyAccessor, UniqueIndexSampler sampler )
    {
        super( writer );
        this.propertyKeyIds = propertyKeyIds;
        this.luceneIndex = luceneIndex;
        this.propertyAccessor = propertyAccessor;
        this.sampler = sampler;
    }

    @Override
    protected void added( IndexEntryUpdate<?> update )
    {
        sampler.increment( 1 );
        updatedValueTuples.add( update.values() );
    }

    @Override
    protected void changed( IndexEntryUpdate<?> update )
    {
        updatedValueTuples.add( update.values() );
    }

    @Override
    protected void removed( IndexEntryUpdate<?> update )
    {
        sampler.increment( -1 );
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        luceneIndex.verifyUniqueness( propertyAccessor, propertyKeyIds, updatedValueTuples );
    }
}
