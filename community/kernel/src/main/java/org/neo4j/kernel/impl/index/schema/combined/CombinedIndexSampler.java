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
package org.neo4j.kernel.impl.index.schema.combined;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;

import static org.neo4j.kernel.impl.index.schema.combined.CombinedSchemaIndexProvider.combineSamples;

class CombinedIndexSampler implements IndexSampler
{
    private final IndexSampler boostSampler;
    private final IndexSampler fallbackSampler;

    CombinedIndexSampler( IndexSampler boostSampler, IndexSampler fallbackSampler )
    {
        this.boostSampler = boostSampler;
        this.fallbackSampler = fallbackSampler;
    }

    @Override
    public IndexSample sampleIndex() throws IndexNotFoundKernelException
    {
        return combineSamples( boostSampler.sampleIndex(), fallbackSampler.sampleIndex() );
    }
}
