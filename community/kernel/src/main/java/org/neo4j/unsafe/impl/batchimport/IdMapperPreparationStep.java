/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.staging.LonelyProcessingStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.staging.Step;

/**
 * Preparation of an {@link IdMapper}, {@link IdMapper#prepare(InputIterable, ProgressListener)}
 * under running as a normal {@link Step} so that normal execution monitoring can be applied.
 * Useful since preparing an {@link IdMapper} can take a significant amount of time.
 */
public class IdMapperPreparationStep extends LonelyProcessingStep
{
    private final IdMapper idMapper;
    private final InputIterable<Object> allIds;

    public IdMapperPreparationStep( StageControl control, int batchSize, int movingAverageSize,
            IdMapper idMapper, InputIterable<Object> allIds )
    {
        super( control, "" /*named later in the progress listener*/, batchSize, movingAverageSize );
        this.idMapper = idMapper;
        this.allIds = allIds;
    }

    @Override
    protected void process()
    {
        idMapper.prepare( allIds, new ProgressListener()
        {
            private final String[] stages = {"SORT", "DETECT", "RESOLVE"};
            private volatile int stage = 0;

            @Override
            public void started()
            {
                resetStats();
                changeName( stages[stage++] );
            }

            @Override
            public void set( long progress )
            {
                throw new UnsupportedOperationException( "Shouldn't be required" );
            }

            @Override
            public void failed( Throwable e )
            {
                issuePanic( e );
            }

            @Override
            public synchronized void add( long progress )
            {   // Directly feed into the progress of this step.
                // Expected to be called by multiple threads, although quite rarely,
                // so synchronization overhead should be negligible.
                progress( progress );
            }

            @Override
            public void done()
            {   // Nothing to do
            }
        } );
    }
}
