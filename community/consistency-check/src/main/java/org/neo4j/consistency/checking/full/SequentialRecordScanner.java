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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.helpers.progress.ProgressMonitorFactory.MultiPartBuilder;
import org.neo4j.kernel.api.direct.BoundedIterable;

public class SequentialRecordScanner<RECORD> extends RecordScanner<RECORD>
{
    public SequentialRecordScanner( String name, Statistics statistics, int threads, BoundedIterable<RECORD> store,
            MultiPartBuilder builder, RecordProcessor<RECORD> processor,
            IterableStore... warmUpStores )
    {
        super( name, statistics, threads, store, builder, processor, warmUpStores );
    }

    @Override
    protected void scan()
    {
        for ( RECORD record : store )
        {
            processor.process( record );
            progress.add( 1 );
        }
    }
}
