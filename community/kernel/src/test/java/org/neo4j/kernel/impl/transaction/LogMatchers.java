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
package org.neo4j.kernel.impl.transaction;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import javax.transaction.xa.Xid;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;

import static org.neo4j.helpers.collection.Iterables.iterable;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

/**
 * A set of hamcrest matchers for asserting logical logs look in certain ways.
 * Please expand as necessary.
 */
public class LogMatchers
{
    public static List<LogEntry> logEntries( FileSystemAbstraction fileSystem, String logPath ) throws IOException
    {
        StoreChannel fileChannel = fileSystem.open( new File( logPath ), "r" );
        ByteBuffer buffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE * 10 );

        // Always a header
        LogHeader header = readLogHeader( buffer, fileChannel, true );

        // Read all log entries
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( fileChannel, header.logVersion, header.logFormatVersion );
        ReadableVersionableLogChannel logChannel =
                new ReadAheadLogChannel( versionedStoreChannel, NO_MORE_CHANNELS, 4096 );
        return Iterables.toList( iterable( new LogEntryCursor( logChannel ) ) );
    }

    public static List<LogEntry> logEntries( FileSystemAbstraction fileSystem, File file ) throws IOException
    {
        return logEntries( fileSystem, file.getPath() );
    }

    public static Matcher<List<LogEntry>> containsExactly( final Matcher<? extends LogEntry>... matchers )
    {
        return new TypeSafeMatcher<List<LogEntry>>()
        {
            @Override
            public boolean matchesSafely( List<LogEntry> item )
            {
                Iterator<LogEntry> actualEntries = item.iterator();
                {
                    for ( Matcher<? extends LogEntry> matcher : matchers )
                    {
                        if ( actualEntries.hasNext() )
                        {
                            LogEntry next = actualEntries.next();
                            if ( !matcher.matches( next ) )
                            {
                                // Wrong!
                                return false;
                            }
                        }
                        else
                        {
                            // Too few actual entries!
                            return false;
                        }
                    }

                    if ( actualEntries.hasNext() )
                    {
                        // Too many actual entries!
                        return false;
                    }

                    // All good in the hood :)
                    return true;
                }
            }

            @Override
            public void describeTo( Description description )
            {
                for ( Matcher<? extends LogEntry> matcher : matchers )
                {
                    description.appendDescriptionOf( matcher ).appendText( ",\n" );
                }
            }
        };
    }

    public static Matcher<? extends LogEntry> startEntry( final int masterId, final int localId )
    {
        return new TypeSafeMatcher<LogEntryStart>()
        {
            @Override
            public boolean matchesSafely( LogEntryStart entry )
            {
                return entry != null && entry.getMasterId() == masterId
                        && entry.getLocalId() == localId;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Start[" + "xid=<Any Xid>,master=" + masterId + ",me=" + localId
                        + ",time=<Any Date>]" );
            }
        };
    }

    public static Matcher<? extends LogEntry> commitEntry( final long txId )
    {
        return new TypeSafeMatcher<OnePhaseCommit>()
        {
            @Override
            public boolean matchesSafely( OnePhaseCommit onePC )
            {
                return onePC != null && onePC.getTxId() == txId;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( String.format( "Commit[txId=%d, <Any Date>]", txId ) );
            }
        };
    }
    public static Matcher<? extends LogEntry> checkPoint( final LogPosition position )
    {
        return new TypeSafeMatcher<CheckPoint>()
        {
            @Override
            public boolean matchesSafely( CheckPoint cp )
            {
                return cp != null &&  position.equals( cp.getLogPosition() );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( String.format( "CheckPoint[position=%s]", position.toString() ) );
            }
        };
    }

    public static Matcher<? extends LogEntry> commandEntry( final long key,
            final Class<? extends Command> commandClass )
    {
        return new TypeSafeMatcher<LogEntryCommand>()
        {
            @Override
            public boolean matchesSafely( LogEntryCommand commandEntry )
            {
                if ( commandEntry == null )
                {
                    return false;
                }

                Command command = commandEntry.getXaCommand();
                return command.getKey() == key &&
                       command.getClass().equals( commandClass );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( String.format( "Command[key=%d, cls=%s]", key, commandClass.getSimpleName() ) );
            }
        };
    }
}
