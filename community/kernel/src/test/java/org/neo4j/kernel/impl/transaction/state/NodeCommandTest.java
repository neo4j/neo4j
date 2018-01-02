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

import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.CommandReader;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogNeoCommandReaderV2_2;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.InMemoryLogChannel;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.kernel.impl.store.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.store.ShortArray.LONG;

import static org.neo4j.kernel.impl.store.record.DynamicRecord.dynamicRecord;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

public class NodeCommandTest
{
    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
    private NodeStore nodeStore;
    InMemoryLogChannel channel = new InMemoryLogChannel();
    private final CommandReader commandReader = new PhysicalLogNeoCommandReaderV2_2();
    private final CommandWriter commandWriter = new CommandWriter( channel );
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
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        nodeCommand.init( before, after );
        assertSerializationWorksFor( nodeCommand );
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
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        nodeCommand.init( before, after );
        assertSerializationWorksFor( nodeCommand );
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
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        nodeCommand.init( before, after );
        assertSerializationWorksFor( nodeCommand );
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
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        nodeCommand.init( before, after );
        assertSerializationWorksFor( nodeCommand );
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
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        nodeCommand.init( before, after );
        assertSerializationWorksFor( nodeCommand );
    }

    @Test
    public void shouldSerializeDynamicRecordsRemoved() throws Exception
    {
        channel.reset();
        // Given
        NodeRecord before = new NodeRecord( 12, false, 1, 2 );
        before.setInUse( true );
        List<DynamicRecord> beforeDyn = singletonList( dynamicRecord(
                0, true, true, -1l, LONG.intValue(), new byte[]{1, 2, 3, 4, 5, 6, 7, 8} ) );
        before.setLabelField( dynamicPointer( beforeDyn ), beforeDyn );
        NodeRecord after = new NodeRecord( 12, false, 2, 1 );
        after.setInUse( true );
        List<DynamicRecord> dynamicRecords = singletonList( dynamicRecord(
                0, false, true, -1l, LONG.intValue(), new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8} ) );
        after.setLabelField( dynamicPointer( dynamicRecords ), dynamicRecords );
        // When
        Command.NodeCommand cmd = new Command.NodeCommand();
        cmd.init( before, after );
        cmd.handle( commandWriter );
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
        cmd.handle( commandWriter );
        Command.NodeCommand result = (Command.NodeCommand) commandReader.read( channel );
        // Then
        assertThat( result, equalTo( cmd ) );
        assertThat( result.getMode(), equalTo( cmd.getMode() ) );
        assertThat( result.getBefore(), equalTo( cmd.getBefore() ) );
        assertThat( result.getAfter(), equalTo( cmd.getAfter() ) );
        // And labels should be the same
        assertThat( labels( result.getBefore() ), equalTo( labels( cmd.getBefore() ) ) );
        assertThat( labels( result.getAfter() ), equalTo( labels( cmd.getAfter() ) ) );
        // And dynamic records should be the same
        assertThat( result.getBefore().getDynamicLabelRecords(), equalTo( result.getBefore().getDynamicLabelRecords() ) );
        assertThat( result.getAfter().getDynamicLabelRecords(), equalTo( result.getAfter().getDynamicLabelRecords() ) );
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
    public void before() throws Exception
    {
        File dir = new File( "dir" );
        fs.get().mkdirs( dir );
        @SuppressWarnings("deprecation")
        StoreFactory storeFactory = new StoreFactory( dir, new Config(), new DefaultIdGeneratorFactory( fs.get() ),
                pageCacheRule.getPageCache( fs.get() ), fs.get(), NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
        nodeStore = neoStores.getNodeStore();
    }

    @After
    public void after() throws Exception
    {
        neoStores.close();
    }
}
