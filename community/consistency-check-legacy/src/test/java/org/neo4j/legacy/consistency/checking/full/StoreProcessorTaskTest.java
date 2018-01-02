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

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.junit.Assert.assertNotSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.legacy.consistency.checking.full.TaskExecutionOrder.MULTI_PASS;
import static org.neo4j.legacy.consistency.checking.full.TaskExecutionOrder.SINGLE_THREADED;

@SuppressWarnings("unchecked")
public class StoreProcessorTaskTest
{
    @Test
    public void singlePassShouldOnlyProcessTheStoreOnce() throws Exception
    {
        // given
        StoreProcessor singlePassProcessor = mock( StoreProcessor.class );
        StoreProcessor multiPassProcessor1 = mock( StoreProcessor.class );
        StoreProcessor multiPassProcessor2 = mock( StoreProcessor.class );

        NodeStore store = mock( NodeStore.class );
        when( store.getStorageFileName() ).thenReturn( new File("node-store") );

        StoreProcessorTask<NodeRecord> task = new StoreProcessorTask<NodeRecord>(
                store, ProgressMonitorFactory.NONE.multipleParts( "check" ), SINGLE_THREADED,
                singlePassProcessor, multiPassProcessor1, multiPassProcessor2 );

        // when
        task.run();

        // then
        verify( singlePassProcessor ).applyFiltered( same( store ), any( ProgressListener.class ) );
        verifyZeroInteractions( multiPassProcessor1, multiPassProcessor2 );
    }

    @Test
    public void multiPassShouldProcessTheStoreOnceForEachOfTheSuppliedProcessors() throws Exception
    {
        // given
        StoreProcessor singlePassProcessor = mock( StoreProcessor.class );
        StoreProcessor multiPassProcessor1 = mock( StoreProcessor.class );
        StoreProcessor multiPassProcessor2 = mock( StoreProcessor.class );

        NodeStore store = mock( NodeStore.class );
        when( store.getStorageFileName() ).thenReturn( new File("node-store") );

        StoreProcessorTask<NodeRecord> task = new StoreProcessorTask<NodeRecord>(
                store, ProgressMonitorFactory.NONE.multipleParts( "check" ), MULTI_PASS,
                singlePassProcessor, multiPassProcessor1, multiPassProcessor2 );

        // when
        task.run();

        // then
        verify( multiPassProcessor1 ).applyFiltered( same( store ), any( ProgressListener.class ) );
        verify( multiPassProcessor2 ).applyFiltered( same( store ), any( ProgressListener.class ) );
        verifyZeroInteractions( singlePassProcessor );
    }

    @Test
    public void multiPassShouldBuildProgressListenersForEachPass() throws Exception
    {
        // given
        StoreProcessor multiPassProcessor1 = mock( StoreProcessor.class );
        StoreProcessor multiPassProcessor2 = mock( StoreProcessor.class );

        NodeStore store = mock( NodeStore.class );
        when( store.getStorageFileName() ).thenReturn( new File("node-store") );

        StoreProcessorTask<NodeRecord> task = new StoreProcessorTask<NodeRecord>(
                store, ProgressMonitorFactory.NONE.multipleParts( "check" ), MULTI_PASS,
                mock( StoreProcessor.class ), multiPassProcessor1, multiPassProcessor2 );

        // when
        task.run();

        // then
        ArgumentCaptor<ProgressListener> listener1 = ArgumentCaptor.forClass( ProgressListener.class );
        ArgumentCaptor<ProgressListener> listener2 = ArgumentCaptor.forClass( ProgressListener.class );
        verify( multiPassProcessor1 ).applyFiltered( same( store ), listener1.capture() );
        verify( multiPassProcessor2 ).applyFiltered( same( store ), listener2.capture() );

        assertNotSame(listener1.getValue(), listener2.getValue());
    }
}
