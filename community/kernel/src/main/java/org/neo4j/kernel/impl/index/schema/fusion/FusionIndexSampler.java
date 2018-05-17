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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.IndexSampler;

public class FusionIndexSampler implements IndexSampler
{
    private final IndexSampler[] samplers;

    public FusionIndexSampler( IndexSampler... samplers )
    {
        this.samplers = samplers;
    }

    @Override
    public IndexSample sampleIndex() throws IndexNotFoundKernelException
    {
        IndexSample[] samples = new IndexSample[samplers.length];
        for ( int i = 0; i < samplers.length; i++ )
        {
            samples[i] = samplers[i].sampleIndex();
        }
        return combineSamples( samples );
    }

    public static IndexSample combineSamples( IndexSample... samples )
    {
        long indexSize = 0;
        long uniqueValues = 0;
        long sampleSize = 0;
        for ( IndexSample sample : samples )
        {
            indexSize += sample.indexSize();
            uniqueValues += sample.uniqueValues();
            sampleSize += sample.sampleSize();
        }
        return new IndexSample( indexSize, uniqueValues, sampleSize );
    }
}
