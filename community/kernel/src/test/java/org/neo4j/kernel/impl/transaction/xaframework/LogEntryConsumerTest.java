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
package org.neo4j.kernel.impl.transaction.xaframework;

import javax.transaction.xa.Xid;

import org.junit.Test;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Functions;
import org.neo4j.kernel.impl.nioneo.xa.command.LogHandler;

import static org.mockito.Mockito.*;

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
        LogEntry.Start start = new LogEntry.Start( mock( Xid.class ), 1, 2, 3, 4, 5, 6 );
        consumer.accept( start );

        // THEN
        verify( handler, times( 1 ) ).startEntry( start );
        verifyNoMoreInteractions( handler );

        // WHEN
        LogEntry.Command command = new LogEntry.Command( 1, null );
        consumer.accept( command );

        // THEN
        verify( handler, times( 1 ) ).commandEntry( command );
        verifyNoMoreInteractions( handler );

        // WHEN
        LogEntry.OnePhaseCommit onePC = new LogEntry.OnePhaseCommit( 1, 2, 3 );
        consumer.accept( onePC );

        // THEN
        verify( handler, times( 1 ) ).onePhaseCommitEntry( onePC );
        verifyNoMoreInteractions( handler );

        // WHEN
        LogEntry.TwoPhaseCommit twoPC = new LogEntry.TwoPhaseCommit( 1, 2, 3 );
        consumer.accept( twoPC );

        // THEN
        verify( handler, times( 1 ) ).twoPhaseCommitEntry( twoPC );
        verifyNoMoreInteractions( handler );

        // WHEN
        LogEntry.Prepare prepare = new LogEntry.Prepare( 1, 2 );
        consumer.accept( prepare );

        // THEN
        verify( handler, times( 1 ) ).prepareEntry( prepare );
        verifyNoMoreInteractions( handler );

        // WHEN
        LogEntry.Done done = new LogEntry.Done( 1 );
        consumer.accept( done );

        // THEN
        verify( handler, times( 1 ) ).doneEntry( done );
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void ensureOldVersionEntriesAreTranslated() throws Exception
    {
        // GIVEN
        Function translator = Functions.identity();

        TranslatingEntryConsumer consumer = new TranslatingEntryConsumer( translator );
        LogHandler handler = mock( LogHandler.class );
        consumer.bind( 0, handler );

        // WHEN
        LogEntry.Start start = new LogEntry.Start( mock( Xid.class ), 1, (byte) (LogEntry.CURRENT_LOG_VERSION + 1), 2, 3, 4, 5, 6 );
        consumer.accept( start );
        LogEntry.Command command = new LogEntry.Command( 1, (byte) (LogEntry.CURRENT_LOG_VERSION + 1),null );
        consumer.accept( command );
        LogEntry.OnePhaseCommit onePC = new LogEntry.OnePhaseCommit( 1, (byte) (LogEntry.CURRENT_LOG_VERSION + 1), 2, 3 );
        consumer.accept( onePC );
        LogEntry.TwoPhaseCommit twoPC = new LogEntry.TwoPhaseCommit( 1, (byte) (LogEntry.CURRENT_LOG_VERSION + 1), 2, 3 );
        consumer.accept( twoPC );
        LogEntry.Prepare prepare = new LogEntry.Prepare( 1, (byte) (LogEntry.CURRENT_LOG_VERSION + 1), 2 );
        consumer.accept( prepare );

        // THEN
        verifyZeroInteractions( handler );

        // WHEN
        LogEntry.Done done = new LogEntry.Done( 1, (byte) (LogEntry.CURRENT_LOG_VERSION + 1) );
        consumer.accept( done );

        // THEN
        verify( handler, times(1) ).startEntry( start );
        verify( handler, times(1) ).commandEntry( command );
        verify( handler, times(1) ).onePhaseCommitEntry( onePC );
        verify( handler, times(1) ).twoPhaseCommitEntry( twoPC );
        verify( handler, times(1) ).prepareEntry( prepare );
        verify( handler, times(1) ).doneEntry( done );
    }
}
