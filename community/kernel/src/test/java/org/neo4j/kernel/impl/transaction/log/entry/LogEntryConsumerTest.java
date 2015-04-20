/*
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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.junit.Test;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.transaction.command.LogHandler;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.log.entry.TranslatingEntryVisitor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class LogEntryConsumerTest
{
    @Test
    public void ensureCurrentVersionEntriesAreHandledImmediately() throws Exception
    {
        // GIVEN
        TranslatingEntryVisitor consumer = new TranslatingEntryVisitor( mock(Function.class) );
        LogHandler handler = mock( LogHandler.class );
        consumer.bind( handler );

        // WHEN
        LogEntryStart start = new LogEntryStart( 1, 2, 3, 4, new byte[1], mock( LogPosition.class ) );
        consumer.visit( start );

        // THEN
        verify( handler, times( 1 ) ).startEntry( start );
        verifyNoMoreInteractions( handler );

        // WHEN
        LogEntryCommand command = new LogEntryCommand( null );
        consumer.visit( command );

        // THEN
        verify( handler, times( 1 ) ).commandEntry( command );
        verifyNoMoreInteractions( handler );

        // WHEN
        OnePhaseCommit onePC = new OnePhaseCommit( 1, 2 );
        consumer.visit( onePC );

        // THEN
        verify( handler, times( 1 ) ).onePhaseCommitEntry( onePC );
        verifyNoMoreInteractions( handler );
    }
}
