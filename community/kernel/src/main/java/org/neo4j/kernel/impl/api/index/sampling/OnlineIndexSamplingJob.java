/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.sampling;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.ValueSampler;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;

class OnlineIndexSamplingJob implements IndexSamplingJob
{
    private final IndexDescriptor indexDescriptor;
    private final IndexProxy indexProxy;
    private final int bufferSize;
    private final IndexStoreView storeView;
    private final StringLogger logger;

    public OnlineIndexSamplingJob( IndexSamplingConfig config,
                                   IndexProxy indexProxy,
                                   IndexStoreView storeView,
                                   Logging logging )
    {
        this.indexDescriptor = indexProxy.getDescriptor();
        this.indexProxy = indexProxy;
        this.bufferSize = config.bufferSize();
        this.storeView = storeView;
        this.logger = logging.getMessagesLog( OnlineIndexSamplingJob.class );
    }

    @Override
    public IndexDescriptor descriptor()
    {
        return indexDescriptor;
    }

    @Override
    public void run()
    {
        try
        {
            try ( IndexReader reader = indexProxy.newReader() )
            {
                ValueSampler sampler = indexProxy.config().isUnique()
                        ? new UniqueIndexSampler()
                        : new NonUniqueIndexSampler( bufferSize );
                reader.sampleIndex( sampler );

                Register.DoubleLongRegister sample = Registers.newDoubleLongRegister();
                final long indexSize = sampler.result( sample );

                // check again if the index is online before saving the counts in the store
                if ( indexProxy.getState() == ONLINE )
                {
                    storeView.replaceIndexCounts( indexDescriptor, sample.readFirst(), sample.readSecond(),
                            indexSize );
                }
            }
        }
        catch ( IndexNotFoundKernelException e )
        {
            logger.warn( "Attempted to sample missing/already deleted index " + indexDescriptor );
        }
    }
}
