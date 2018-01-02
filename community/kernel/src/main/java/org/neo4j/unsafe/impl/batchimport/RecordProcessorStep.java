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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.staging.Step;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

/**
 * {@link RecordProcessor} in {@link Step Step-form}.
 */
public class RecordProcessorStep<T extends AbstractBaseRecord> extends ProcessorStep<T[]>
{
    private final RecordProcessor<T> processor;
    private final boolean endOfLine;

    public RecordProcessorStep( StageControl control, String name, Configuration config,
            RecordProcessor<T> processor, boolean endOfLine, StatsProvider... additionalStatsProviders )
    {
        super( control, name, config, 1, additionalStatsProviders );
        this.processor = processor;
        this.endOfLine = endOfLine;
    }

    @Override
    protected void process( T[] batch, BatchSender sender )
    {
        for ( T item : batch )
        {
            if ( item.inUse() )
            {
                processor.process( item );
            }
        }

        // This step can be used in different stage settings, possible as the last step,
        // where nothing should be emitted
        if ( !endOfLine )
        {
            sender.send( batch );
        }
    }

    @Override
    protected void done()
    {
        super.done();
        processor.done();
    }
}
