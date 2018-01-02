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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.staging.Step;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;

public class PropertyEncoderStepTest
{
    public final @Rule EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    public final @Rule PageCacheRule pageCacheRule = new PageCacheRule();
    private NeoStores neoStores;
    private PageCache pageCache;

    @Before
    public void setUpNeoStore()
    {
        File storeDir = new File( "dir" );
        pageCache = pageCacheRule.getPageCache( fsRule.get() );
        StoreFactory storeFactory =
                new StoreFactory( fsRule.get(), storeDir, pageCache, NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
    }

    @After
    public void closeNeoStore() throws IOException
    {
        neoStores.close();
        pageCache.close();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldGrowPropertyBlocksArrayProperly() throws Exception
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
        BatchingPropertyKeyTokenRepository tokens =
                new BatchingPropertyKeyTokenRepository( neoStores.getPropertyKeyTokenStore(), 0 );
        Step<Batch<InputNode,NodeRecord>> step =
                new PropertyEncoderStep<>( control, DEFAULT, tokens, neoStores.getPropertyStore() );
        @SuppressWarnings( "rawtypes" )
        Step downstream = mock( Step.class );
        step.setDownstream( downstream );

        // WHEN
        step.start( 0 );
        step.receive( 0, smallbatch() );
        step.endOfUpstream();
        awaitCompleted( step, control );

        // THEN
        verify( downstream ).receive( anyLong(), any() );
        verifyNoMoreInteractions( control );
    }

    private void awaitCompleted( Step<?> step, StageControl control ) throws InterruptedException
    {
        while ( !step.isCompleted() )
        {
            Thread.sleep( 10 );
            verifyNoMoreInteractions( control );
        }
    }

    private Batch<InputNode,NodeRecord> smallbatch()
    {
        return new Batch<>( new InputNode[] {new InputNode( "source", 1, 0, "1", new Object[] {
                "key1", "value1",
                "key2", "value2",
                "key3", "value3",
                "key4", "value4",
                "key5", "value5"
        }, null, new String[] {
                "label1",
                "label2",
                "label3",
                "label4"
        }, null )} );
    }
}
