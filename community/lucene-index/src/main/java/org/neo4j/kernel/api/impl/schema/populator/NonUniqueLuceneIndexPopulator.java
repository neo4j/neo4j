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
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.kernel.api.impl.schema.SchemaIndex;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.NonUniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;

/**
 * A {@link LuceneIndexPopulator} used for non-unique Lucene schema indexes.
 * Performs sampling using {@link NonUniqueIndexSampler}.
 */
public class NonUniqueLuceneIndexPopulator extends LuceneIndexPopulator
{
    private final NonUniqueIndexSampler sampler;

    public NonUniqueLuceneIndexPopulator( SchemaIndex luceneIndex, IndexSamplingConfig samplingConfig )
    {
        super( luceneIndex );
        this.sampler = new NonUniqueIndexSampler( samplingConfig.sampleSizeLimit() );
    }

    @Override
    public void verifyDeferredConstraints( PropertyAccessor accessor ) throws IndexEntryConflictException, IOException
    {
        // no constraints to verify so do nothing
    }

    @Override
    public IndexUpdater newPopulatingUpdater( PropertyAccessor propertyAccessor ) throws IOException
    {
        return new NonUniqueLuceneIndexPopulatingUpdater( writer, sampler );
    }

    @Override
    public void includeSample( NodePropertyUpdate update )
    {
        sampler.include( LuceneDocumentStructure.encodedStringValue( update.getValueAfter() ) );
    }

    @Override
    public IndexSample sampleResult()
    {
        return sampler.result();
    }
}
