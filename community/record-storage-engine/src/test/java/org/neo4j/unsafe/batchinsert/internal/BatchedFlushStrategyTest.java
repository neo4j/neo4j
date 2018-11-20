/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.unsafe.batchinsert.internal;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.neo4j.kernel.impl.store.record.DirectRecordAccessSet;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

class BatchedFlushStrategyTest
{
    @Test
    void testFlush()
    {
        DirectRecordAccessSet recordAccessSet = Mockito.mock( DirectRecordAccessSet.class );
        BatchInserterImpl.BatchedFlushStrategy flushStrategy = createFlushStrategy( recordAccessSet, 2 );
        flushStrategy.flush();
        verifyZeroInteractions( recordAccessSet );
        flushStrategy.flush();
        verify( recordAccessSet ).commit();
        reset( recordAccessSet );

        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();

        verify( recordAccessSet, Mockito.times( 3 ) ).commit();
    }

    @Test
    void testForceFlush()
    {
        DirectRecordAccessSet recordAccessSet = Mockito.mock( DirectRecordAccessSet.class );
        BatchInserterImpl.BatchedFlushStrategy flushStrategy = createFlushStrategy( recordAccessSet, 2 );

        flushStrategy.forceFlush();
        flushStrategy.forceFlush();
        verify( recordAccessSet, Mockito.times( 2 ) ).commit();

        flushStrategy.flush();
        flushStrategy.forceFlush();
        verify( recordAccessSet, Mockito.times( 3 ) ).commit();
    }

    @Test
    void testResetBatchCounterOnForce()
    {
        DirectRecordAccessSet recordAccessSet = Mockito.mock( DirectRecordAccessSet.class );
        BatchInserterImpl.BatchedFlushStrategy flushStrategy = createFlushStrategy( recordAccessSet, 3 );

        flushStrategy.flush();
        flushStrategy.flush();
        verifyZeroInteractions( recordAccessSet );

        flushStrategy.forceFlush();
        verify( recordAccessSet ).commit();
        verifyNoMoreInteractions( recordAccessSet );

        flushStrategy.flush();
        flushStrategy.flush();
    }

    private BatchInserterImpl.BatchedFlushStrategy createFlushStrategy( DirectRecordAccessSet recordAccessSet, int batchSize )
    {
        return new BatchInserterImpl.BatchedFlushStrategy( recordAccessSet, batchSize );
    }
}
