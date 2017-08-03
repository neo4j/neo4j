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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;

class FusionIndexSampler implements IndexSampler
{
    private final IndexSampler nativeSampler;
    private final IndexSampler luceneSampler;

    FusionIndexSampler( IndexSampler nativeSampler, IndexSampler luceneSampler )
    {
        this.nativeSampler = nativeSampler;
        this.luceneSampler = luceneSampler;
    }

    @Override
    public IndexSample sampleIndex() throws IndexNotFoundKernelException
    {
        return combineSamples( nativeSampler.sampleIndex(), luceneSampler.sampleIndex() );
    }

    static IndexSample combineSamples( IndexSample first, IndexSample other )
    {
        return new IndexSample(
                first.indexSize() + other.indexSize(),
                first.uniqueValues() + other.uniqueValues(),
                first.sampleSize() + other.sampleSize() );
    }
}
