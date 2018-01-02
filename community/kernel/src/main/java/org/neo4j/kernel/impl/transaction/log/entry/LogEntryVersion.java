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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.kernel.impl.transaction.command.CommandHandler;
import org.neo4j.kernel.impl.transaction.command.CommandReader;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV1_9;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_0;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_1;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2_10;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2_4;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogNeoCommandReaderV2_2;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;

import static java.lang.String.format;

/**
 * Main entry point into log entry versions and parsers and all that. A {@link LogEntryVersion} can be retrieved
 * by using {@link #byVersion(byte, byte)} and from there get a hold of
 * {@link LogEntryParser} using {@link #entryParser(byte)} and
 * {@link CommandReader} using {@link #newCommandReader()}.
 *
 * Here follows an explanation how log entry versioning in Neo4j works:
 *
 * In Neo4j transactions are written to a log. Each transaction consists of one or more log entries. Log entries
 * can be of one or more types, such as denoting start of a transaction, commands and committing the transaction.
 * Neo4j supports writing the latest/current log entry and reading log entries for all currently supported versions
 * of Neo4j. The way versioning is done has changed over the years.
 *   First there was a format header of the entire log and it was assumed that all log entries within that log
 * was of the same format. This version actually specified command version, i.e. just versions of one of the
 * log entry types. This was a bit clunky and forced format specification to be passed in from outside,
 * based on the log that was read and so updated every time a new log was opened.
 *   Starting with Neo4j version 2.1 a one-byte log entry version was introduced with every single log entry.
 * This allowed for more flexible reading and simpler code. Versions started with negative number to be able to
 * distinguish the new format from the non-versioned format. So observing the log entry type, which was the first
 * byte in each log entry being negative being negative was a signal for the new format and that the type actually
 * was the next byte. This to support rolling upgrades where two Neo4j versions in a cluster could be active
 * simultaneously, yet talking in terms of log entries of different versions.
 *
 * At this point in time there was the log entry version which signalled how an entry was to be read, but there
 * was still the log-global format version which didn't really do anything but make things complicated in the code.
 *   As of 2.2.4 the log-global format version is gone, although still just a token value written to adhere to
 * the 16 bytes header size of a log for backwards compatibility. The log entry version controls everything
 * about versioning of log entries and commands, such that if either log entry format (such as log entry types,
 * such as START, COMMIT and the likes, or data within them) change, or one or more command format change
 * the log entry verision will be bumped.
 *   The process of making an update to log entry or command format is to NOT change any existing class, but to:
 * <ol>
 * <li>Copy {@link PhysicalLogCommandReaderV2_2_4} or similar and modify the new copy</li>
 * <li>Copy {@link LogEntryParsersV2_2_4} or similar and modify the new copy if entry layout has changed</li>
 * <li>Add an entry in this enum, like {@link #V2_2_4} pointing to the above new classes</li>
 * <li>Modify {@link CommandWriter}. Also {@link LogEntryWriter} (if log entry layout has changed)
 * with required changes</li>
 * <li>Change {@link #CURRENT} to point to the newly created version</li>
 * </ol>
 * Everything apart from that should just work and Neo4j should automatically support the new version as well.
 *
 * The {@link #newCommandReader()} creates a new instance every time. An example of a {@link CommandReader}
 * is {@link PhysicalLogCommandReaderV2_2_4}. They are really (sort of) stateless, but the way they
 * are implemented, via the use of {@link CommandHandler} the channel to read from must be injected by
 * constructor or other means. That's why they have to be created at certain points and retained as long
 * as it makes sense, local to the reading thread.
 *
 * We need to keep going the negative version number route until all supported versions have negative
 * version numbers. Then we can have the next version be positive and we should get rid of the:
 * read, check < 0, read again thing in {@link VersionAwareLogEntryReader}.
 */
public enum LogEntryVersion
{
    // as of 2011-10-17
    V1_9( 0, LogEntryParsersV1_9.class, 2 )
    {
        @Override
        public CommandReader newCommandReader()
        {
            return new PhysicalLogCommandReaderV1_9();
        }
    },
    // as of 2013-02-09: neo4j 2.0 Labels & Indexing
    V2_0( 0, LogEntryParsersV2_0.class, 3 )
    {
        @Override
        public CommandReader newCommandReader()
        {
            return new PhysicalLogCommandReaderV2_0();
        }
    },
    // as of 2014-02-06: neo4j 2.1 Dense nodes, split by type/direction into groups
    V2_1( -1, LogEntryParsersV2_1.class )
    {
        @Override
        public CommandReader newCommandReader()
        {
            return new PhysicalLogCommandReaderV2_1();
        }
    },
    // as of 2014-05-23: neo4j 2.2 Removal of JTA / unified data source
    V2_2( -2, LogEntryParsersV2_2.class )
    {
        @Override
        public CommandReader newCommandReader()
        {
            return new PhysicalLogNeoCommandReaderV2_2();
        }
    },
    // as of 2015-07-23: neo4j 2.2.4 legacy index command header has bigger id space
    // -4 is correct, -3 can be found in some 2.3 milestones that's why we play it safe
    V2_2_4( -4, LogEntryParsersV2_2_4.class )
    {
        @Override
        public CommandReader newCommandReader()
        {
            return new PhysicalLogCommandReaderV2_2_4();
        }
    },
    V2_3( -5, LogEntryParsersV2_3.class )
    {
        @Override
        public CommandReader newCommandReader()
        {
            return new PhysicalLogCommandReaderV2_2_4();
        }
    },
    // as of 2016-05-27: neo4j 2.2.10 legacy index IndexDefineCommand maps write size as short instead of byte
    // -7 is picked, -5 and -6 can be found in the future (2.3 and 3.0)
    // log entry layout hasn't changed since 2_2_4 so just use that one
    V2_2_10( -7, LogEntryParsersV2_2_4.class )
    {
        @Override
        public CommandReader newCommandReader()
        {
            return new PhysicalLogCommandReaderV2_2_10();
        }
    },
    // as of 2016-05-30: neo4j 2.3.5 legacy index IndexDefineCommand maps write size as short instead of byte
    // See comment for V2.2.10 for version number explanation
    // log entry layout hasn't changed since 2_3 so just use that one
    V2_3_5( -8, LogEntryParsersV2_3.class )
    {
        @Override
        public CommandReader newCommandReader()
        {
            return new PhysicalLogCommandReaderV2_2_10();
        }
    };

    public static final LogEntryVersion CURRENT = V2_3_5;
    public static final byte NO_PARTICULAR_LOG_HEADER_FORMAT_VERSION = -1;
    private static final LogEntryVersion[] ALL = values();
    private static final LogEntryVersion[] NEGATIVE = new LogEntryVersion[ALL.length+5]; // pessimistic size
    private static final LogEntryVersion[] POSITIVE = new LogEntryVersion[ALL.length+5]; // pessimistic size
    static
    {
        for ( LogEntryVersion version : ALL )
        {
            if ( version.byteCode() < 0 )
            {
                put( NEGATIVE, -version.byteCode(), version );
            }
            else
            {
                put( POSITIVE, version.byteCode(), version );
            }
        }
    }

    private final byte version;
    private final LogEntryParser<LogEntry>[] entryTypes;
    private final byte logHeaderFormatVersion;

    /**
     * A little trick to be able to keep multiple versions of the same {@link #byteCode()} in the same array
     * index. They will form a linked list and be matched against {@link #logHeaderFormatVersion()}.
     */
    private LogEntryVersion nextWithSameLogEntryVersion;

    private LogEntryVersion( int version,
            Class<? extends Enum<? extends LogEntryParser<? extends LogEntry>>> cls )
    {
        this( version, cls, NO_PARTICULAR_LOG_HEADER_FORMAT_VERSION );
    }

    @SuppressWarnings( "unchecked" )
    private LogEntryVersion( int version,
            Class<? extends Enum<? extends LogEntryParser<? extends LogEntry>>> cls, int logHeaderFormatVersion )
    {
        this.logHeaderFormatVersion = safeCastToByte( logHeaderFormatVersion );
        this.entryTypes = new LogEntryParser[highestCode( cls ) + 1];
        for ( Enum<? extends LogEntryParser<? extends LogEntry>> parser : cls.getEnumConstants() )
        {
            LogEntryParser<LogEntry> candidate = (LogEntryParser<LogEntry>) parser;
            this.entryTypes[candidate.byteCode()] = candidate;
        }
        this.version = safeCastToByte( version );
    }

    /**
     * @return byte value representing this log entry version.
     */
    public byte byteCode()
    {
        return version;
    }

    /**
     * @param type type of entry.
     * @return a {@link LogEntryParser} capable of reading a {@link LogEntry} of the given type for this
     * log entry version.
     */
    public LogEntryParser<LogEntry> entryParser( byte type )
    {
        LogEntryParser<LogEntry> candidate = (type >= 0 && type < entryTypes.length) ? entryTypes[type] : null;
        if ( candidate == null )
        {
            throw new IllegalArgumentException( "Unknown entry type " + type + " for version " + version );
        }
        return candidate;
    }

    /**
     * Why do we create {@link CommandReader}s like this? The only reason is that we're using the {@link CommandHandler}
     * as interface to drive the reading, and those methods doesn't accept the source of information as argument,
     * i.e. in this case the channel. It's causing head aches actually. Perhaps we should include some genericified
     * source in those calls as well?
     *
     * Please cache a returned instances so that there ain't one instance created per log entry.
     */
    public abstract CommandReader newCommandReader();

    /**
     * Return the correct {@link LogEntryVersion} for the given {@code version} code read from f.ex a log entry.
     * There's a tie between {@link #V1_9} and {@link #V2_0} that needs to be broken by
     * {@code logHeaderFormatVersion} as long as those versions are supported. They have the same,
     * i.e. no log entry version. When we no longer supported those we can get rid of that parameter here.
     * Lookup is fast and can be made inside critical paths, no need for externally caching the returned
     * {@link LogEntryVersion} instance per the input arguments.
     *
     * @param version log entry version
     * @param logHeaderFormatVersion used to break tie between {@link #V1_9} and {@link #V2_0} as
     * long as we still support those versions.
     */
    public static LogEntryVersion byVersion( byte version, byte logHeaderFormatVersion )
    {
        byte flattenedVersion;
        LogEntryVersion[] from;
        if ( version < 0 )
        {
            from = NEGATIVE;
            flattenedVersion = (byte) -version;
        }
        else
        {
            from = POSITIVE;
            flattenedVersion = version;
        }

        LogEntryVersion candidate = (flattenedVersion < from.length) ? from[flattenedVersion] : null;

        // Match against logHeaderFormatVersion. Remove this once we drop support for either 1.9 or 2.0
        while ( candidate != null )
        {
            // If our candidate is the only one for this version code then we don't need/want
            // to additionally match the logHeaderFormatVersion. Otherwise we must do that.
            if ( candidate.nextWithSameLogEntryVersion == null ||
                    candidate.logHeaderFormatVersion == logHeaderFormatVersion )
            {
                return candidate;
            }
            candidate = candidate.nextWithSameLogEntryVersion;
        }
        throw new IllegalArgumentException( "Unrecognized log entry version " + version +
                " and logHeaderFormatVersion " + logHeaderFormatVersion );
    }

    byte logHeaderFormatVersion()
    {
        return logHeaderFormatVersion;
    }

    private static void put( LogEntryVersion[] array, int index, LogEntryVersion version )
    {
        version.nextWithSameLogEntryVersion = array[index];
        array[index] = version;
    }

    private static byte safeCastToByte( int value )
    {
        boolean reversed = false;
        if ( value < 0 )
        {
            value = ~value;
            reversed = true;
        }

        if ( (value & ~0xFF) != 0 )
        {
            throw new Error( format( "Bad version %d, must be contained within one byte", value ) );
        }
        return (byte) (reversed ? ~value : value);
    }

    @SuppressWarnings( "unchecked" )
    private static int highestCode( Class<? extends Enum<? extends LogEntryParser<? extends LogEntry>>> cls )
    {
        int highestCode = 0;
        for ( Enum<? extends LogEntryParser<? extends LogEntry>> parser : cls.getEnumConstants() )
        {
            LogEntryParser<LogEntry> candidate = (LogEntryParser<LogEntry>) parser;
            highestCode = Math.max( highestCode, candidate.byteCode() );
        }
        return highestCode;
    }
}
