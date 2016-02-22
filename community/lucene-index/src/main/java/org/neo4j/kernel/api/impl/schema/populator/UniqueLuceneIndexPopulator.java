/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndex;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;

/**
 * A {@link LuceneIndexPopulator} used for unique Lucene schema indexes.
 * Performs sampling using {@link UniqueIndexSampler}.
 * Verifies uniqueness of added and changed values using
 * {@link LuceneSchemaIndex#verifyUniqueness(PropertyAccessor, int)} method.
 */
public class UniqueLuceneIndexPopulator extends LuceneIndexPopulator
{
    private final int propertyKeyId;
    private final UniqueIndexSampler sampler;

    public UniqueLuceneIndexPopulator( LuceneSchemaIndex index, IndexDescriptor descriptor )
    {
        super( index );
        this.propertyKeyId = descriptor.getPropertyKeyId();
        this.sampler = new UniqueIndexSampler();
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
    {
        luceneIndex.verifyUniqueness( accessor, propertyKeyId );
    }

    @Override
    public IndexUpdater newPopulatingUpdater( final PropertyAccessor accessor ) throws IOException
    {
        return new UniqueLuceneIndexPopulatingUpdater( writer, propertyKeyId, luceneIndex, accessor, sampler );
    }

    @Override
    public void includeSample( NodePropertyUpdate update )
    {
        sampler.increment( 1 );
    }

    @Override
    public IndexSample sampleResult()
    {
        return sampler.result();
    }
}
