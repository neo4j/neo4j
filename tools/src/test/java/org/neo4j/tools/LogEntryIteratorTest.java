/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.FixedIOCursor;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

public class LogEntryIteratorTest
{
    @Test
    public void shouldBeEmptyForEmptyLogEntryCursor() throws IOException
    {
        // Given
        LogEntryIterator iterator = newIterator();

        // Then
        assertFalse( iterator.hasNext() );
        try
        {
            iterator.next();
            fail( "Exception expected" );
        }
        catch ( NoSuchElementException ignore )
        {
        }
    }

    @Test
    public void shouldContainSingleEntryForSingleEntryCursor() throws IOException
    {
        // Given
        LogEntryStart logEntryStart = new LogEntryStart( 1, 1, 1L, 1L, new byte[0], LogPosition.UNSPECIFIED );
        LogEntryIterator iterator = newIterator( logEntryStart );

        // Then
        assertEquals( Collections.singletonList( logEntryStart ), Iterables.toList( iterator ) );
    }

    @Test
    public void shouldContainCorrectEntriesForMultiItemCursor() throws IOException
    {
        // Given
        LogEntryStart start = new LogEntryStart( 1, 1, 1L, 1L, new byte[0], LogPosition.UNSPECIFIED );
        LogEntryCommand command1 = new LogEntryCommand( new Command.NodeCommand().init(
                new NodeRecord( 1, false, false, -1, -1, -1 ),
                new NodeRecord( 1, true, false, -1, -1, -1 ) ) );
        LogEntryCommand command2 = new LogEntryCommand( new Command.PropertyCommand().init(
                new PropertyRecord( 1 ),
                new PropertyRecord( 1, new NodeRecord( 1 ) )
        ) );
        LogEntryCommand command3 = new LogEntryCommand( new Command.RelationshipCommand().init(
                new RelationshipRecord( 1, 1, 2, 3 )
        ) );
        OnePhaseCommit commit = new OnePhaseCommit( 1L, System.currentTimeMillis() );

        LogEntryIterator iterator = newIterator( start, command1, command2, command3, commit );

        // Then
        assertEquals( Arrays.asList( start, command1, command2, command3, commit ), Iterables.toList( iterator ) );
    }

    private static LogEntryIterator newIterator( final LogEntry... entries ) throws IOException
    {
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class, RETURNS_MOCKS );

        return new LogEntryIterator( fs, new File( "logs" ) )
        {
            @Override
            IOCursor<LogEntry> newLogEntryCursor( StoreChannel storeChannel ) throws IOException
            {
                return new FixedIOCursor<>( entries );
            }
        };
    }
}
