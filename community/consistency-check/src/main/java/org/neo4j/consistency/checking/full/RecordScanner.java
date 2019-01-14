/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;

abstract class RecordScanner<RECORD> extends ConsistencyCheckerTask
{
    protected final ProgressListener progress;
    protected final BoundedIterable<RECORD> store;
    protected final RecordProcessor<RECORD> processor;
    private final IterableStore[] warmUpStores;

    RecordScanner( String name, Statistics statistics, int threads, BoundedIterable<RECORD> store,
            ProgressMonitorFactory.MultiPartBuilder builder, RecordProcessor<RECORD> processor,
            IterableStore... warmUpStores )
    {
        super( name, statistics, threads );
        this.store = store;
        this.processor = processor;
        long maxCount = store.maxCount();
        this.progress = maxCount == -1
                ? builder.progressForUnknownPart( name )
                : builder.progressForPart( name, maxCount );
        this.warmUpStores = warmUpStores;
    }

    @Override
    public void run()
    {
        statistics.reset();
        if ( warmUpStores != null )
        {
            for ( IterableStore store : warmUpStores )
            {
                store.warmUpCache();
            }
        }
        try
        {
            scan();
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
                throw new RuntimeException( e );
            }
            finally
            {
                processor.close();
                progress.done();
            }
        }
        statistics.print( name );
    }

    protected abstract void scan();
}
