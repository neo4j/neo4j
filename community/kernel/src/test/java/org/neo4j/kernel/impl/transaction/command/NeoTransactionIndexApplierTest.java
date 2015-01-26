/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.state.LazyIndexUpdates;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.EXTERNAL;

public class NeoTransactionIndexApplierTest
{
    private final IndexingService indexingService = mock( IndexingService.class );
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final PropertyStore propertyStore = mock( PropertyStore.class );
    private final CacheAccessBackDoor cacheAccess = mock( CacheAccessBackDoor.class );
    private final PropertyLoader propertyLoader = mock( PropertyLoader.class );

    private final Map<Long,Command.NodeCommand> emptyNodeCommands = Collections.emptyMap();
    private final Map<Long,List<Command.PropertyCommand>> emptyPropCommands = Collections.emptyMap();

    private final long transactionId = 42;
    private WorkSync<LabelScanStore,IndexTransactionApplier.LabelUpdateWork>
            labelScanStoreSynchronizer = new WorkSync<>( labelScanStore );

    @Test
    public void shouldUpdateIndexesOnNodeCommands() throws IOException
    {
        // given
        final IndexTransactionApplier applier = new IndexTransactionApplier( indexingService,
                nodeStore, propertyStore, cacheAccess, propertyLoader, transactionId, EXTERNAL,
                labelScanStoreSynchronizer );

        final NodeRecord before = new NodeRecord( 11 );
        final NodeRecord after = new NodeRecord( 12 );
        final Command.NodeCommand command = new Command.NodeCommand().init( before, after );

        // when
        final boolean result = applier.visitNodeCommand( command );
        applier.apply();

        // then
        assertFalse( result );

        final Map<Long,Command.NodeCommand> nodeCommands = Collections.singletonMap( command.getKey(), command );

        final LazyIndexUpdates expectedUpdates = new LazyIndexUpdates(
                nodeStore, propertyStore, emptyPropCommands, nodeCommands, propertyLoader );

        verify( indexingService, times( 1 ) ).updateIndexes( eq( expectedUpdates ), eq( transactionId ), eq( false ) );
    }

    @Test
    public void shouldUpdateLabelStoreScanOnNodeCommands() throws IOException
    {
        // given
        final IndexTransactionApplier applier = new IndexTransactionApplier( indexingService,
                nodeStore, propertyStore, cacheAccess, propertyLoader, transactionId, EXTERNAL,
                labelScanStoreSynchronizer );

        final NodeRecord before = new NodeRecord( 11 );
        before.setLabelField( 17, Collections.<DynamicRecord>emptySet() );
        final NodeRecord after = new NodeRecord( 12 );
        after.setLabelField( 18, Collections.<DynamicRecord>emptySet() );
        final Command.NodeCommand command = new Command.NodeCommand().init( before, after );

        when( labelScanStore.newWriter() ).thenReturn( mock( LabelScanWriter.class ) );

        // when
        final boolean result = applier.visitNodeCommand( command );
        applier.apply();

        // then
        assertFalse( result );

        final NodeLabelUpdate update = NodeLabelUpdate.labelChanges( command.getKey(), new long[]{}, new long[]{} );
        final Collection<NodeLabelUpdate> labelUpdates = Arrays.asList( update );

        verify( cacheAccess, times( 1 ) ).applyLabelUpdates( eq( labelUpdates ) );

        final Map<Long,Command.NodeCommand> nodeCommands = Collections.singletonMap( command.getKey(), command );

        final LazyIndexUpdates expectedUpdates = new LazyIndexUpdates(
                nodeStore, propertyStore, emptyPropCommands, nodeCommands, propertyLoader );

        verify( indexingService, times( 1 ) ).updateIndexes( eq( expectedUpdates ), eq( transactionId ), eq( false ) );
    }

    @Test
    public void shouldUpdateIndexesOnPropertyCommandsWhenThePropertyIsOnANode() throws IOException
    {
        // given
        final IndexTransactionApplier applier = new IndexTransactionApplier( indexingService,
                nodeStore, propertyStore, cacheAccess, propertyLoader, transactionId, EXTERNAL,
                labelScanStoreSynchronizer );

        final PropertyRecord before = new PropertyRecord( 11 );
        final PropertyRecord after = new PropertyRecord( 12 );
        after.setNodeId( 42 );
        final Command.PropertyCommand command = new Command.PropertyCommand().init( before, after );

        // when
        final boolean result = applier.visitPropertyCommand( command );
        applier.apply();

        // then
        assertFalse( result );


        Map<Long,List<Command.PropertyCommand>> propCommands =
                Collections.singletonMap( command.getNodeId(), Arrays.asList( command ) );

        final LazyIndexUpdates expectedUpdates = new LazyIndexUpdates(
                nodeStore, propertyStore, propCommands, emptyNodeCommands, propertyLoader );

        verify( indexingService, times( 1 ) ).updateIndexes( eq( expectedUpdates ), eq( transactionId ), eq( false ) );
    }

    @Test
    public void shouldNotUpdateIndexesOnPropertyCommandsWhenThePropertyIsNotOnANode() throws IOException
    {
        // given
        final IndexTransactionApplier applier = new IndexTransactionApplier( indexingService,
                nodeStore, propertyStore, cacheAccess, propertyLoader, transactionId, EXTERNAL,
                labelScanStoreSynchronizer );

        final PropertyRecord before = new PropertyRecord( 11 );
        final PropertyRecord after = new PropertyRecord( 12 );
        final Command.PropertyCommand command = new Command.PropertyCommand().init( before, after );

        // when
        final boolean result = applier.visitPropertyCommand( command );
        applier.apply();

        // then
        assertFalse( result );
        verify( indexingService, never() ).updateIndexes( Matchers.<LazyIndexUpdates>any(), anyLong(), anyBoolean() );
    }
}
