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
package org.neo4j.kernel.impl.transaction.command;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand;

public class PhysicalLogCommandReadersTest
{
    private static final long ID = 42;
    private static final byte IN_USE_FLAG = Record.IN_USE.byteValue();
    private static final short TYPE = (short) (Short.MAX_VALUE + 42);
    private static final int TYPE_AS_INT = TYPE & 0xFFFF;
    private static final long NEXT = 42;
    private static final long FIRST_OUT = 42;
    private static final long FIRST_IN = 42;
    private static final long FIRST_LOOP = 42;
    private static final long OWNING_NODE = 42;

    @Test
    public void readRelGroupWithHugeTypeInV1_9()
    {
        assertDoesNotKnowAboutRelGroups( new PhysicalLogCommandReaderV1_9() );
    }

    @Test
    public void readRelGroupWithHugeTypeInV2_0()
    {
        assertDoesNotKnowAboutRelGroups( new PhysicalLogCommandReaderV2_0() );
    }

    @Test
    public void readRelGroupWithHugeTypeInV2_1() throws IOException
    {
        assertCanReadRelGroup( new PhysicalLogCommandReaderV2_1() );
    }

    @Test
    public void readRelGroupWithHugeTypeInV2_2() throws IOException
    {
        assertCanReadRelGroup( new PhysicalLogNeoCommandReaderV2_2() );
    }

    @Test
    public void readRelGroupWithHugeTypeInV2_2_4() throws IOException
    {
        assertCanReadRelGroup( new PhysicalLogCommandReaderV2_2_4() );
    }

    @Test
    public void readRelGroupWithHugeTypeInV2_2_10() throws IOException
    {
        assertCanReadRelGroup( new PhysicalLogCommandReaderV2_2_10() );
    }

    private static void assertDoesNotKnowAboutRelGroups( CommandReader reader )
    {
        try
        {
            reader.read( channelWithRelGroupRecord() );
            fail( "Exception expected" );
        }
        catch ( IOException e )
        {
            assertEquals( "Unknown command type[" + NeoCommandType.REL_GROUP_COMMAND + "]", e.getMessage() );
        }
    }

    private void assertCanReadRelGroup( CommandReader reader ) throws IOException
    {
        Command command = reader.read( channelWithRelGroupRecord() );
        assertValidRelGroupCommand( command );
    }

    private static void assertValidRelGroupCommand( Command command )
    {
        assertThat( command, instanceOf( RelationshipGroupCommand.class ) );
        RelationshipGroupCommand relGroupCommand = (RelationshipGroupCommand) command;
        RelationshipGroupRecord record = relGroupCommand.getRecord();

        assertEquals( ID, record.getId() );
        if ( IN_USE_FLAG == Record.IN_USE.byteValue() )
        {
            assertTrue( record.inUse() );
        }
        else if ( IN_USE_FLAG == Record.NOT_IN_USE.byteValue() )
        {
            assertFalse( record.inUse() );
        }
        else
        {
            throw new IllegalStateException( "Illegal inUse flag: " + IN_USE_FLAG );
        }
        assertEquals( TYPE_AS_INT, record.getType() );
        assertEquals( NEXT, record.getNext() );
        assertEquals( FIRST_OUT, record.getFirstOut() );
        assertEquals( FIRST_IN, record.getFirstIn() );
        assertEquals( FIRST_LOOP, record.getNext() );
        assertEquals( OWNING_NODE, record.getOwningNode() );
    }

    private static ReadableLogChannel channelWithRelGroupRecord() throws IOException
    {
        return channelWithRelGroupRecord( ID, IN_USE_FLAG, TYPE, NEXT, FIRST_OUT, FIRST_IN, FIRST_LOOP, OWNING_NODE );
    }

    private static ReadableLogChannel channelWithRelGroupRecord( long id, byte inUse, short type, long next,
            long firstOut, long firstIn, long firstLoop, long owningNode ) throws IOException
    {
        ReadableLogChannel channel = mock( ReadableLogChannel.class );

        when( channel.get() ).thenReturn( NeoCommandType.REL_GROUP_COMMAND ).thenReturn( inUse );
        when( channel.getLong() ).thenReturn( id ).thenReturn( next ).thenReturn( firstOut ).thenReturn( firstIn )
                .thenReturn( firstLoop ).thenReturn( owningNode );
        when( channel.getShort() ).thenReturn( type );

        return channel;
    }
}
