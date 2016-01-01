/**
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
package org.neo4j.kernel.impl.nioneo.xa;

import static java.nio.ByteBuffer.allocate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandReaderV1;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandWriter;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;

public class RelationshipGroupCommandTest
{
    private NodeStore nodeStore;
    private XaCommandReader commandReader = new PhysicalLogNeoXaCommandReaderV1( allocate( 64 ));
    private XaCommandWriter commandWriter = new PhysicalLogNeoXaCommandWriter();

    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Before
    public void before() throws Exception
    {
        @SuppressWarnings("deprecation")
        StoreFactory storeFactory = new StoreFactory( new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs.get(), StringLogger.DEV_NULL, new DefaultTxHook() );
        File storeFile = new File( "story" );
        storeFactory.createNodeStore( storeFile );
        nodeStore = storeFactory.newNodeStore( storeFile );
    }

    @After
    public void after() throws Exception
    {
        nodeStore.close();
    }

    @Test
    public void shouldSerializeAndDeserializeUnusedRecords() throws Exception
    {
        // Given
        RelationshipGroupRecord record = new RelationshipGroupRecord( 10, 12 );
        record.setInUse( false );

        // When
        Command.RelationshipGroupCommand command = new Command.RelationshipGroupCommand();
        command.init( record );
        assertSerializationWorksFor( command );
    }

    @Test
    public void shouldSerializeCreatedRecord() throws Exception
    {
        // Given
        RelationshipGroupRecord record = new RelationshipGroupRecord( 10, 12 );
        record.setCreated();
        record.setInUse( true );

        // When
        Command.RelationshipGroupCommand command = new Command.RelationshipGroupCommand();
        command.init( record );
        assertSerializationWorksFor( command );
    }

    private void assertSerializationWorksFor( Command.RelationshipGroupCommand cmd ) throws IOException
    {
        InMemoryLogBuffer buffer = new InMemoryLogBuffer();
        commandWriter.write( cmd, buffer );
        Command.RelationshipGroupCommand result = (Command.RelationshipGroupCommand) commandReader.read( buffer );

        RelationshipGroupRecord recordBefore = cmd.getRecord();
        RelationshipGroupRecord recordAfter = result.getRecord();

        // Then
        assertThat( recordBefore.getFirstIn(), equalTo( recordAfter.getFirstIn() ) );
        assertThat( recordBefore.getFirstOut(), equalTo( recordAfter.getFirstOut() ) );
        assertThat( recordBefore.getFirstLoop(), equalTo( recordAfter.getFirstLoop() ) );
        assertThat( recordBefore.getNext(), equalTo( recordAfter.getNext() ) );
        assertThat( recordBefore.getOwningNode(), equalTo( recordAfter.getOwningNode() ) );
        assertThat( recordBefore.getPrev(), equalTo( recordAfter.getPrev() ) );
        assertThat( recordBefore.getType(), equalTo( recordAfter.getType() ) );
    }
}
