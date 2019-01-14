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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV3_0;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.store.ShortArray.LONG;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;
import static org.neo4j.kernel.impl.store.record.DynamicRecord.dynamicRecord;

public class NodeCommandTest
{
    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    private NodeStore nodeStore;
    InMemoryClosableChannel channel = new InMemoryClosableChannel();
    private final CommandReader commandReader = new PhysicalLogCommandReaderV3_0();
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private NeoStores neoStores;

    @Test
    public void shouldSerializeAndDeserializeUnusedRecords() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, false, 1, 2 );
        NodeRecord after = new NodeRecord( 12, false, 2, 1 );
        // When
        assertSerializationWorksFor( new Command.NodeCommand( before, after ) );
    }

    @Test
    public void shouldSerializeCreatedRecord() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, false, 1, 2 );
        NodeRecord after = new NodeRecord( 12, false, 2, 1 );
        after.setCreated();
        after.setInUse( true );
        // When
        assertSerializationWorksFor( new Command.NodeCommand( before, after ) );
    }

    @Test
    public void shouldSerializeDenseRecord() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, false, 1, 2 );
        before.setInUse( true );
        NodeRecord after = new NodeRecord( 12, true, 2, 1 );
        after.setInUse( true );
        // When
        assertSerializationWorksFor( new Command.NodeCommand( before, after ) );
    }

    @Test
    public void shouldSerializeUpdatedRecord() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, false, 1, 2 );
        before.setInUse( true );
        NodeRecord after = new NodeRecord( 12, false, 2, 1 );
        after.setInUse( true );
        // When
        assertSerializationWorksFor( new Command.NodeCommand( before, after ) );
    }

    @Test
    public void shouldSerializeInlineLabels() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, false, 1, 2 );
        before.setInUse( true );
        NodeRecord after = new NodeRecord( 12, false, 2, 1 );
        after.setInUse( true );
        NodeLabels nodeLabels = parseLabelsField( after );
        nodeLabels.add( 1337, nodeStore, nodeStore.getDynamicLabelStore() );
        // When
        assertSerializationWorksFor( new Command.NodeCommand( before, after ) );
    }

    @Test
    public void shouldSerializeSecondaryUnitUsage() throws Exception
    {
        // Given
        // a record that is changed to include a secondary unit
        NodeRecord before = new NodeRecord( 13, false, 1, 2 );
        before.setInUse( true );
        before.setRequiresSecondaryUnit( false );
        before.setSecondaryUnitId( NO_ID ); // this and the previous line set the defaults, they are here for clarity
        NodeRecord after = new NodeRecord( 13, false, 1, 2 );
        after.setInUse( true );
        after.setRequiresSecondaryUnit( true );
        after.setSecondaryUnitId( 14L );

        Command.NodeCommand command = new Command.NodeCommand( before, after );

        // Then
        assertSerializationWorksFor( command );
    }

    @Test
    public void shouldSerializeDynamicRecordLabels() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, false, 1, 2 );
        before.setInUse( true );
        NodeRecord after = new NodeRecord( 12, false, 2, 1 );
        after.setInUse( true );
        NodeLabels nodeLabels = parseLabelsField( after );
        for ( int i = 10; i < 100; i++ )
        {
            nodeLabels.add( i, nodeStore, nodeStore.getDynamicLabelStore() );
        }
        // When
        assertSerializationWorksFor( new Command.NodeCommand( before, after ) );
    }

    @Test
    public void shouldSerializeDynamicRecordsRemoved() throws Exception
    {
        channel.reset();
        // Given
        NodeRecord before = new NodeRecord( 12, false, 1, 2 );
        before.setInUse( true );
        List<DynamicRecord> beforeDyn = singletonList( dynamicRecord(
                0, true, true, -1L, LONG.intValue(), new byte[]{1, 2, 3, 4, 5, 6, 7, 8} ) );
        before.setLabelField( dynamicPointer( beforeDyn ), beforeDyn );
        NodeRecord after = new NodeRecord( 12, false, 2, 1 );
        after.setInUse( true );
        List<DynamicRecord> dynamicRecords = singletonList( dynamicRecord(
                0, false, true, -1L, LONG.intValue(), new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8} ) );
        after.setLabelField( dynamicPointer( dynamicRecords ), dynamicRecords );
        // When
        Command.NodeCommand cmd = new Command.NodeCommand( before, after );
        cmd.serialize( channel );
        Command.NodeCommand result = (Command.NodeCommand) commandReader.read( channel );
        // Then
        assertThat( result, equalTo( cmd ) );
        assertThat( result.getMode(), equalTo( cmd.getMode() ) );
        assertThat( result.getBefore(), equalTo( cmd.getBefore() ) );
        assertThat( result.getAfter(), equalTo( cmd.getAfter() ) );
        // And dynamic records should be the same
        assertThat( result.getBefore().getDynamicLabelRecords(), equalTo( cmd.getBefore().getDynamicLabelRecords() ) );
        assertThat( result.getAfter().getDynamicLabelRecords(), equalTo( cmd.getAfter().getDynamicLabelRecords() ) );
    }

    private void assertSerializationWorksFor( org.neo4j.kernel.impl.transaction.command.Command.NodeCommand cmd )
            throws IOException
    {
        channel.reset();
        cmd.serialize( channel );
        Command.NodeCommand result = (Command.NodeCommand) commandReader.read( channel );
        // Then
        assertThat( result, equalTo( cmd ) );
        assertThat( result.getMode(), equalTo( cmd.getMode() ) );
        assertThat( result.getBefore(), equalTo( cmd.getBefore() ) );
        assertThat( result.getAfter(), equalTo( cmd.getAfter() ) );
        // And created and dense flags should be the same
        assertThat( result.getBefore().isCreated(), equalTo( cmd.getBefore().isCreated() ) );
        assertThat( result.getAfter().isCreated(), equalTo( cmd.getAfter().isCreated() ) );
        assertThat( result.getBefore().isDense(), equalTo( cmd.getBefore().isDense() ) );
        assertThat( result.getAfter().isDense(), equalTo( cmd.getAfter().isDense()) );
        // And labels should be the same
        assertThat( labels( result.getBefore() ), equalTo( labels( cmd.getBefore() ) ) );
        assertThat( labels( result.getAfter() ), equalTo( labels( cmd.getAfter() ) ) );
        // And dynamic records should be the same
        assertThat( result.getBefore().getDynamicLabelRecords(), equalTo( cmd.getBefore().getDynamicLabelRecords() ) );
        assertThat( result.getAfter().getDynamicLabelRecords(), equalTo( cmd.getAfter().getDynamicLabelRecords() ) );
        // And the secondary unit information should be the same
        // Before
        assertThat( result.getBefore().requiresSecondaryUnit(), equalTo( cmd.getBefore().requiresSecondaryUnit() ) );
        assertThat( result.getBefore().hasSecondaryUnitId(), equalTo( cmd.getBefore().hasSecondaryUnitId() ) );
        assertThat( result.getBefore().getSecondaryUnitId(), equalTo( cmd.getBefore().getSecondaryUnitId() ) );
        // and after
        assertThat( result.getAfter().requiresSecondaryUnit(), equalTo( cmd.getAfter().requiresSecondaryUnit() ) );
        assertThat( result.getAfter().hasSecondaryUnitId(), equalTo( cmd.getAfter().hasSecondaryUnitId() ) );
        assertThat( result.getAfter().getSecondaryUnitId(), equalTo( cmd.getAfter().getSecondaryUnitId() ) );
    }

    private Set<Integer> labels( NodeRecord record )
    {
        long[] rawLabels = parseLabelsField( record ).get( nodeStore );
        Set<Integer> labels = new HashSet<>( rawLabels.length );
        for ( long label : rawLabels )
        {
            labels.add( safeCastLongToInt( label ) );
        }
        return labels;
    }

    @Before
    public void before()
    {
        File dir = new File( "dir" );
        fs.get().mkdirs( dir );
        @SuppressWarnings( "deprecation" )
        StoreFactory storeFactory = new StoreFactory( dir, Config.defaults(), new DefaultIdGeneratorFactory( fs.get() ),
                pageCacheRule.getPageCache( fs.get() ), fs.get(), NullLogProvider.getInstance(),
                EmptyVersionContextSupplier.EMPTY );
        neoStores = storeFactory.openAllNeoStores( true );
        nodeStore = neoStores.getNodeStore();
    }

    @After
    public void after()
    {
        neoStores.close();
    }
}
