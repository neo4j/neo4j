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
package org.neo4j.kernel.impl.transaction.xaframework.log.entry;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableVersionableLogChannel;

import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogVersions.CURRENT_LOG_VERSION;

/**
 * Version aware factory of LogEntryReaders
 * <p/>
 * Starting with Neo4j version 2.2, we can read older format log versions at runtime. Support for this comes from
 * {@link org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryParserFactory} which can provide different
 * parsers for the log entries.
 * <p/>
 * Starting with Neo4j version 2.1, log entries are prefixed with a version. This allows for Neo4j instances of
 * different versions to exchange transaction data, either directly or via logical logs. This implementation of
 * LogEntryReader makes use of the version information to deserialize command entries that hold commands created
 * with previous versions of Neo4j. Support for this comes from the required
 * {@link org.neo4j.kernel.impl.nioneo.xa.CommandReaderFactory} which can provide deserializers for Commands given
 * the version.
 */
public class LogEntryReaderFactory
{
    private final LogEntryParserFactory logEntryParserFactory;
    private final CommandReaderFactory commandReaderFactory;

    public LogEntryReaderFactory()
    {
        this( new DefaultLogEntryParserFactory(), new CommandReaderFactory.Default() );
    }

    public LogEntryReaderFactory( LogEntryParserFactory logEntryParserFactory,
                                  CommandReaderFactory commandReaderFactory )
    {
        this.logEntryParserFactory = logEntryParserFactory;
        this.commandReaderFactory = commandReaderFactory;
    }

    public <T extends ReadableVersionableLogChannel> LogEntryReader<T> versionable()
    {
        final VersionAwareLogEntryReader reader =
                new VersionAwareLogEntryReader( logEntryParserFactory, commandReaderFactory );

        return new LogEntryReader<T>()
        {
            @Override
            public LogEntry readLogEntry( T channel ) throws IOException
            {
                return reader.readLogEntry( channel, channel.getLogFormatVersion() );
            }
        };
    }

    public <T extends ReadableLogChannel> LogEntryReader<T> create()
    {
        final VersionAwareLogEntryReader reader =
                new VersionAwareLogEntryReader( logEntryParserFactory, commandReaderFactory );

        return new LogEntryReader<T>()
        {
            @Override
            public LogEntry readLogEntry( T channel ) throws IOException
            {
                return reader.readLogEntry( channel, CURRENT_LOG_VERSION );
            }
        };
    }
}
