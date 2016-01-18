/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Flushes stores after a batch of records has been applied.
 */
public class StoreFlusherStep extends ProcessorStep<Batch<?,?>>
{
    private final RecordStore<?>[] stores;

    public StoreFlusherStep( StageControl control, Configuration config, RecordStore<?>...stores )
    {
        super( control, "FLUSH", config, 1 );
        this.stores = stores;
    }

    @Override
    protected void process( Batch<?,?> batch, BatchSender sender ) throws Throwable
    {
        // Flush after every batch.
        // We get vectored, sequential IO when we write with flush, plus it makes future page faulting faster.
        for ( RecordStore<?> store : stores )
        {
            store.flush();
        }
    }
}
