/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.state;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.causalclustering.core.replication.DistributedOperation;
import org.neo4j.function.ThrowingBiConsumer;

class CommandBatcher
{
    private List<DistributedOperation> batch;
    private int maxBatchSize;
    private final ThrowingBiConsumer<Long,List<DistributedOperation>,Exception> applier;
    private long lastIndex;

    CommandBatcher( int maxBatchSize, ThrowingBiConsumer<Long,List<DistributedOperation>,Exception> applier )
    {
        this.batch = new ArrayList<>( maxBatchSize );
        this.maxBatchSize = maxBatchSize;
        this.applier = applier;
    }

    void add( long index, DistributedOperation operation ) throws Exception
    {
        if ( batch.size() > 0 )
        {
            assert index == (lastIndex + 1);
        }

        batch.add( operation );
        lastIndex = index;

        if ( batch.size() == maxBatchSize )
        {
            flush();
        }
    }

    void flush() throws Exception
    {
        applier.accept( lastIndex, batch );
        batch.clear();
    }
}
