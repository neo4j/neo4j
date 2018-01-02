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
package org.neo4j.unsafe.batchinsert;

import org.junit.Test;
import org.mockito.Mockito;

public class BatchedFlushStrategyTest
{

    @Test
    public void testFlush() throws Exception
    {
        DirectRecordAccessSet recordAccessSet = Mockito.mock( DirectRecordAccessSet.class );
        BatchInserterImpl.BatchedFlushStrategy flushStrategy = createFlushStrategy( recordAccessSet, 2 );
        flushStrategy.flush();
        Mockito.verifyZeroInteractions( recordAccessSet );
        flushStrategy.flush();
        Mockito.verify( recordAccessSet ).commit();
        Mockito.reset( recordAccessSet );

        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();
        flushStrategy.flush();

        Mockito.verify( recordAccessSet, Mockito.times( 3 ) ).commit();
    }

    @Test
    public void testForceFlush() throws Exception
    {
        DirectRecordAccessSet recordAccessSet = Mockito.mock( DirectRecordAccessSet.class );
        BatchInserterImpl.BatchedFlushStrategy flushStrategy = createFlushStrategy( recordAccessSet, 2 );

        flushStrategy.forceFlush();
        flushStrategy.forceFlush();
        Mockito.verify( recordAccessSet, Mockito.times( 2 ) ).commit();

        flushStrategy.flush();
        flushStrategy.forceFlush();
        Mockito.verify( recordAccessSet, Mockito.times( 3 ) ).commit();
    }

    @Test
    public void testResetBatchCounterOnForce()
    {
        DirectRecordAccessSet recordAccessSet = Mockito.mock( DirectRecordAccessSet.class );
        BatchInserterImpl.BatchedFlushStrategy flushStrategy = createFlushStrategy( recordAccessSet, 3 );

        flushStrategy.flush();
        flushStrategy.flush();
        Mockito.verifyZeroInteractions( recordAccessSet );

        flushStrategy.forceFlush();
        Mockito.verify( recordAccessSet ).commit();
        Mockito.verifyNoMoreInteractions( recordAccessSet );

        flushStrategy.flush();
        flushStrategy.flush();
    }

    private BatchInserterImpl.BatchedFlushStrategy createFlushStrategy( DirectRecordAccessSet recordAccessSet, int
            batchSize )
    {
        return new BatchInserterImpl.BatchedFlushStrategy( recordAccessSet, batchSize );
    }
}
