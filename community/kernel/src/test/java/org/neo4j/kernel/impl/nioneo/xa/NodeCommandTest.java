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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;

import static java.nio.ByteBuffer.allocate;
import static java.util.Arrays.asList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import static org.neo4j.kernel.impl.nioneo.store.DynamicRecord.dynamicRecord;
import static org.neo4j.kernel.impl.nioneo.store.ShortArray.LONG;
import static org.neo4j.kernel.impl.nioneo.store.labels.DynamicNodeLabels.dynamicPointer;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;
import static org.neo4j.kernel.impl.nioneo.xa.Command.readCommand;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

public class NodeCommandTest
{
    private NodeStore nodeStore;
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldSerializeAndDeserializeUnusedRecords() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, 1, 2 );
        NodeRecord after = new NodeRecord( 12, 2, 1 );

        // When
        assertSerializationWorksFor( new Command.NodeCommand( null, before, after ) );
    }

    @Test
    public void shouldSerializeCreatedRecord() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, 1, 2 );
        NodeRecord after = new NodeRecord( 12, 2, 1 );
        after.setCreated();
        after.setInUse( true );

        // When
        assertSerializationWorksFor( new Command.NodeCommand( null, before, after ) );
    }

    @Test
    public void shouldSerializeUpdatedRecord() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, 1, 2 );
        before.setInUse( true );
        NodeRecord after = new NodeRecord( 12, 2, 1 );
        after.setInUse( true );

        // When
        assertSerializationWorksFor( new Command.NodeCommand( null, before, after ) );
    }

    @Test
    public void shouldSerializeInlineLabels() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, 1, 2 );
        before.setInUse( true );

        NodeRecord after = new NodeRecord( 12, 2, 1 );
        after.setInUse( true );
        NodeLabels nodeLabels = parseLabelsField( after );
        nodeLabels.add( 1337, nodeStore );

        // When
        assertSerializationWorksFor( new Command.NodeCommand( null, before, after ) );
    }

    @Test
    public void shouldSerializeDynamicRecordLabels() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, 1, 2 );
        before.setInUse( true );

        NodeRecord after = new NodeRecord( 12, 2, 1 );
        after.setInUse( true );
        NodeLabels nodeLabels = parseLabelsField( after );
        for ( int i = 10; i < 100; i++ )
        {
            nodeLabels.add( i, nodeStore );
        }

        // When
        assertSerializationWorksFor( new Command.NodeCommand( null, before, after ) );
    }

    @Test
    public void shouldSerializeDynamicRecordsRemoved() throws Exception
    {
        // Given
        NodeRecord before = new NodeRecord( 12, 1, 2 );
        before.setInUse( true );
        List<DynamicRecord> beforeDyn = asList( dynamicRecord( 0, true, true, -1l, LONG.intValue(), new byte[]{1,2,3,4,5,6,7,8}));
        before.setLabelField( dynamicPointer( beforeDyn ), beforeDyn );

        NodeRecord after = new NodeRecord( 12, 2, 1 );
        after.setInUse( true );
        List<DynamicRecord> dynamicRecords = asList( dynamicRecord( 0, false, true, -1l, LONG.intValue(), new byte[]{1,2,3,4,5,6,7,8}));
        after.setLabelField( dynamicPointer( dynamicRecords ), dynamicRecords );

        // When
        Command.NodeCommand cmd = new Command.NodeCommand( null, before, after );
        InMemoryLogBuffer buffer = new InMemoryLogBuffer();
        cmd.writeToFile( buffer );
        Command.NodeCommand result = (Command.NodeCommand) readCommand( null, null, buffer, allocate( 64 ) );

        // Then
        assertThat( result, equalTo( cmd ) );
        assertThat( result.getMode(), equalTo( cmd.getMode() ) );
        assertThat( result.getBefore(), equalTo( cmd.getBefore() ) );
        assertThat( result.getAfter(), equalTo( cmd.getAfter() ) );

        // And dynamic records should be the same
        assertThat( result.getBefore().getDynamicLabelRecords(), equalTo( cmd.getBefore().getDynamicLabelRecords()));
        assertThat( result.getAfter().getDynamicLabelRecords(), equalTo( cmd.getAfter().getDynamicLabelRecords() ) );
    }

    private void assertSerializationWorksFor( Command.NodeCommand cmd ) throws IOException
    {
        InMemoryLogBuffer buffer = new InMemoryLogBuffer();
        cmd.writeToFile( buffer );
        Command.NodeCommand result = (Command.NodeCommand) readCommand( null, null, buffer, allocate( 64 ) );

        // Then
        assertThat( result, equalTo( cmd ) );
        assertThat( result.getMode(), equalTo( cmd.getMode() ) );
        assertThat( result.getBefore(), equalTo( cmd.getBefore() ) );
        assertThat( result.getAfter(), equalTo( cmd.getAfter() ) );

        // And labels should be the same
        assertThat( labels( result.getBefore() ), equalTo( labels( cmd.getBefore() ) ) );
        assertThat( labels( result.getAfter() ), equalTo( labels( cmd.getAfter() ) ) );

        // And dynamic records should be the same
        assertThat( result.getBefore().getDynamicLabelRecords(), equalTo( result.getBefore().getDynamicLabelRecords()));
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
        @SuppressWarnings("deprecation")
        StoreFactory storeFactory = new StoreFactory( new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs.get(), StringLogger.DEV_NULL, new DefaultTxHook() );
        File storeFile = new File( "nodestore" );
        storeFactory.createNodeStore( storeFile );
        nodeStore = storeFactory.newNodeStore( storeFile );
    }

    @After
    public void after() throws Exception
    {
        nodeStore.close();
    }
}
