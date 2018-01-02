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
package org.neo4j.legacy.consistency.checking.full;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.direct.BoundedIterable;

public class RecordScanner<RECORD> implements StoppableRunnable
{
    private final ProgressListener progress;
    private final BoundedIterable<RECORD> store;
    private final RecordProcessor<RECORD> processor;

    private volatile boolean continueScanning = true;

    public RecordScanner( BoundedIterable<RECORD> store,
                          String taskName,
                          ProgressMonitorFactory.MultiPartBuilder builder,
                          RecordProcessor<RECORD> processor )
    {
        this.store = store;
        this.processor = processor;
        this.progress = builder.progressForPart( taskName, store.maxCount() );
    }

    @Override
    public void run()
    {
        try
        {
            int entryCount = 0;
            for ( RECORD record : store )
            {
                if ( !continueScanning )
                {
                    return;
                }
                processor.process( record );
                progress.set( entryCount++ );
            }
        }
        finally
        {
            try
            {
                store.close();
            }
            catch ( Exception e )
            {
                progress.failed( e );
            }
            processor.close();
            progress.done();
        }
    }

    @Override
    public void stopScanning()
    {
        continueScanning = false;
    }
}
