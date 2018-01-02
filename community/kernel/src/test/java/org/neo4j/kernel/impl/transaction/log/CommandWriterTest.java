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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.command.CommandReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommandWriterTest
{
    @Test
    public void shouldWriteIndexDefineCommandIfMapWithinShortRange() throws IOException
    {
        // GIVEN
        InMemoryLogChannel channel =  new InMemoryLogChannel( 10_000 );
        CommandWriter commandWriter = new CommandWriter( channel );
        IndexDefineCommand command = initIndexDefineCommand( 300 );

        // WHEN
        boolean b = commandWriter.visitIndexDefineCommand( command );
        assertFalse( b );

        // THEN
        CommandReader commandReader = LogEntryVersion.CURRENT.newCommandReader();
        IndexDefineCommand read = (IndexDefineCommand) commandReader.read( channel );
        assertEquals( command.getIndexNameIdRange(), read.getIndexNameIdRange() );
        assertEquals( command.getKeyIdRange(), read.getKeyIdRange() );
    }

    @Test
    public void shouldFailToWriteIndexDefineCommandIfMapIsLargerThanShort() throws IOException
    {
        // GIVEN
        InMemoryLogChannel channel =  new InMemoryLogChannel( 1000 );
        CommandWriter commandWriter = new CommandWriter( channel );
        IndexDefineCommand command = mock( IndexDefineCommand.class );
        Map<String,Integer> largeMap = initMap( 0xFFFF + 1 );
        when( command.getIndexNameIdRange() ).thenReturn( largeMap );
        when( command.getKeyIdRange() ).thenReturn( largeMap );

        // WHEN
        try
        {
            commandWriter.visitIndexDefineCommand( command );
            fail( "Expected an AssertionError" );
        }
        catch ( AssertionError e )
        {
            // THEN Fine
        }
    }

    private IndexDefineCommand initIndexDefineCommand( int nbrOfEntries )
    {
        IndexDefineCommand command = new IndexDefineCommand();
        Map<String,Integer> indexNames = initMap( nbrOfEntries );
        Map<String,Integer> keys = initMap( nbrOfEntries );
        command.init( indexNames, keys );
        return command;
    }

    private Map<String,Integer> initMap( int nbrOfEntries )
    {
        Map<String, Integer> toReturn = new HashMap<>();
        while ( nbrOfEntries-- > 0 )
        {
            toReturn.put( "key" + nbrOfEntries, nbrOfEntries );
        }
        return toReturn;
    }
}
