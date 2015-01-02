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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;

public class PhysicalNeoCommandReaderV0_19Test
{
    @Test
    public void testReturnsRelationshipCommandsWithProperFirstInChainFlags() throws Exception
    {
        PhysicalLogNeoCommandReaderV0_19 reader = new PhysicalLogNeoCommandReaderV0_19();

        RelationshipRecord writtenRecord = new RelationshipRecord( 1, 2, 3, 4 );
        writtenRecord.setInUse( true );
        writtenRecord.setSecondPrevRel( 3 );
        writtenRecord.setFirstPrevRel( Record.NO_PREV_RELATIONSHIP.intValue() );
        writtenRecord.setFirstInSecondChain( false ); // this is for OCD reasons, doesn't affect test
        ReadableLogChannel mockChannel = mock( ReadableLogChannel.class );
        when( mockChannel.get() ).thenReturn( NeoCommandType.REL_COMMAND ).thenReturn( (byte) 1 );
        when( mockChannel.getLong() )
                .thenReturn( writtenRecord.getId() )
                .thenReturn( writtenRecord.getFirstNode() )
                .thenReturn( writtenRecord.getSecondNode() )
                .thenReturn( writtenRecord.getFirstPrevRel() )
                .thenReturn( writtenRecord.getFirstNextRel() )
                .thenReturn( writtenRecord.getSecondPrevRel() )
                .thenReturn( writtenRecord.getSecondNextRel() )
                .thenReturn( writtenRecord.getNextProp() )
                ;
        when( mockChannel.getInt() ).thenReturn( writtenRecord.getType() );

        Command result = reader.read( mockChannel );

        assertTrue( result instanceof Command.RelationshipCommand );

        Command.RelationshipCommand relCommand = (Command.RelationshipCommand) result;
        RelationshipRecord readRecord = relCommand.getRecord();
        assertTrue( readRecord.isFirstInFirstChain() );
        assertFalse( readRecord.isFirstInSecondChain() );
    }
}
