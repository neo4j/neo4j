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
package org.neo4j.kernel.impl.api.index.sampling;

import org.neo4j.kernel.impl.util.DurationLogger;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static java.lang.String.format;
import static org.neo4j.kernel.api.index.InternalIndexState.ONLINE;

class OnlineIndexSamplingJob implements IndexSamplingJob
{
    private final IndexDescriptor indexDescriptor;
    private final IndexProxy indexProxy;
    private final IndexStoreView storeView;
    private final Log log;
    private final String indexUserDescription;

    public OnlineIndexSamplingJob( IndexProxy indexProxy,
                                   IndexStoreView storeView,
                                   String indexUserDescription,
                                   LogProvider logProvider )
    {
        this.indexDescriptor = indexProxy.getDescriptor();
        this.indexProxy = indexProxy;
        this.storeView = storeView;
        this.log = logProvider.getLog( getClass() );
        this.indexUserDescription = indexUserDescription;
    }

    @Override
    public IndexDescriptor descriptor()
    {
        return indexDescriptor;
    }

    @Override
    public void run()
    {
        try( DurationLogger durationLogger = new DurationLogger( log, "Sampling index " + indexUserDescription ) )
        {
            try
            {
                try ( IndexReader reader = indexProxy.newReader() )
                {
                    Register.DoubleLongRegister sample = Registers.newDoubleLongRegister();
                    final long indexSize = reader.sampleIndex( sample );

                    // check again if the index is online before saving the counts in the store
                    if ( indexProxy.getState() == ONLINE )
                    {
                        long unique = sample.readFirst();
                        long sampleSize = sample.readSecond();
                        storeView.replaceIndexCounts( indexDescriptor, unique, sampleSize, indexSize );
                        durationLogger.markAsFinished();
                        log.info(
                            format( "Sampled index %s with %d unique values in sample of avg size %d taken from " +
                                    "index containing %d entries",
                                    indexUserDescription, unique, sampleSize, indexSize ) );
                    }
                    else
                    {
                        durationLogger.markAsAborted( "Index no longer ONLINE" );
                    }
                }
            }
            catch ( IndexNotFoundKernelException e )
            {
                durationLogger.markAsAborted(
                        "Attempted to sample missing/already deleted index " + indexUserDescription );
            }
        }
    }

}
