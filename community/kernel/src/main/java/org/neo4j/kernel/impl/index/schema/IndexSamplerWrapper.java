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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.DefaultNonUniqueIndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.UniqueIndexSampler;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.Value;

class IndexSamplerWrapper
{
    private final DefaultNonUniqueIndexSampler generalSampler;
    private final UniqueIndexSampler uniqueSampler;

    IndexSamplerWrapper( SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        switch ( descriptor.type() )
        {
        case GENERAL:
            generalSampler = new DefaultNonUniqueIndexSampler( samplingConfig.sampleSizeLimit() );
            uniqueSampler = null;
            break;
        case UNIQUE:
            generalSampler = null;
            uniqueSampler = new UniqueIndexSampler();
            break;
        default:
            throw new UnsupportedOperationException( "Unexpected index type " + descriptor.type() );
        }
    }

    void includeSample( Value[] values )
    {
        if ( uniqueSampler != null )
        {
            uniqueSampler.increment( 1 );
        }
        else
        {
            generalSampler.include( SamplingUtil.encodedStringValuesForSampling( (Object[]) values ) );
        }
    }

    IndexSample sampleResult()
    {
        if ( uniqueSampler != null )
        {
            return uniqueSampler.result();
        }
        else
        {
            return generalSampler.result();
        }
    }
}
