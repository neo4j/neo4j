/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Test;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;

public class LogEntryConsumerTest
{
    @Test
    public void ensureCurrentVersionEntriesAreHandledImmediately() throws Exception
    {
        // GIVEN
        TranslatingEntryConsumer consumer = new TranslatingEntryConsumer( mock(Function.class) );
        LogHandler handler = mock( LogHandler.class );
        consumer.bind( 0, handler );

        // WHEN
        LogEntry.Start start = new LogEntry.Start( 1, 2, 3, 4, new byte[1], mock( LogPosition.class ) );
        consumer.accept( start );

        // THEN
        verify( handler, times( 1 ) ).startEntry( start );
        verifyNoMoreInteractions( handler );

        // WHEN
        LogEntry.Command command = new LogEntry.Command( null );
        consumer.accept( command );

        // THEN
        verify( handler, times( 1 ) ).commandEntry( command );
        verifyNoMoreInteractions( handler );

        // WHEN
        LogEntry.OnePhaseCommit onePC = new LogEntry.OnePhaseCommit( 1, 2 );
        consumer.accept( onePC );

        // THEN
        verify( handler, times( 1 ) ).onePhaseCommitEntry( onePC );
        verifyNoMoreInteractions( handler );
    }
}
