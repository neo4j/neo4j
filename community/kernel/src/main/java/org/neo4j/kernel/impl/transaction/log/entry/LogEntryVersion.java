/*
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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV2_2_4;
import org.neo4j.storageengine.api.StorageCommand;

import static java.lang.String.format;

/**
 * Main entry point into log entry versions and parsers and all that. A {@link LogEntryVersion} can be retrieved
 * by using {@link #byVersion(byte, byte)} and from there get a hold of
 * {@link LogEntryParser} using {@link #entryParser(byte)}.
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
 * At this point in time there was the log entry version which signaled how an entry was to be read, but there
 * was still the log-global format version which didn't really do anything but make things complicated in the code.
 *   As of 2.2.4 the log-global format version is gone, although still just a token value written to adhere to
 * the 16 bytes header size of a log for backwards compatibility. The log entry version controls everything
 * about versioning of log entries and commands, such that if either log entry format (such as log entry types,
 * such as START, COMMIT and the likes, or data within them) change, or one or more command format change
 * the log entry verision will be bumped.
 * The process of making an update to log entry or command format is to:
 * <ol>
 * <li>Copy {@link PhysicalLogCommandReaderV2_2_4} or similar and modify the new copy</li>
 * <li>Copy {@link LogEntryParsersV2_3} or similar and modify the new copy</li>
 * <li>Add an entry in this enum, like {@link #V2_2_4} pointing to the above new classes</li>
 * <li>Modify the appropriate {@link StorageCommand} and {@link LogEntryWriter} with required changes</li>
 * </ol>
 * Everything apart from that should just work and Neo4j should automatically support the new version as well.
 *
 * We need to keep going the negative version number route until all supported versions have negative
 * version numbers. Then we can have the next version be positive and we should get rid of the:
 * read, check < 0, read again thing in {@link VersionAwareLogEntryReader}.
 */
public enum LogEntryVersion
{
    // as of 2013-02-09: neo4j 2.0 Labels & Indexing
    V2_0( 0, LogEntryParsersV2_0.class ),
    // as of 2014-02-06: neo4j 2.1 Dense nodes, split by type/direction into groups
    V2_1( -1, LogEntryParsersV2_1.class ),
    // as of 2014-05-23: neo4j 2.2 Removal of JTA / unified data source
    V2_2( -2, LogEntryParsersV2_2.class ),
    // as of 2015-07-23: neo4j 2.2.4 legacy index command header has bigger id space
    // -4 is correct, -3 can be found in some 2.3 milestones that's why we play it safe
    V2_2_4( -4, LogEntryParsersV2_2_4.class ),
    V2_3( -5, LogEntryParsersV2_3.class ),
    V3_0( -6, LogEntryParsersV2_3.class );

    public static final LogEntryVersion CURRENT = V3_0;
    private static final LogEntryVersion[] ALL = values();
    private static final LogEntryVersion[] LOOKUP_BY_VERSION = new LogEntryVersion[ALL.length + 1]; // pessimistic size
    static
    {
        for ( LogEntryVersion version : ALL )
        {
            put( LOOKUP_BY_VERSION, -version.byteCode(), version );
        }
    }

    private final byte version;
    private final LogEntryParser<LogEntry>[] entryTypes;

    LogEntryVersion( int version, Class<? extends Enum<? extends LogEntryParser<? extends LogEntry>>> cls )
    {
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
     * Return the correct {@link LogEntryVersion} for the given {@code version} code read from f.ex a log entry.
     * Lookup is fast and can be made inside critical paths, no need for externally caching the returned
     * {@link LogEntryVersion} instance per the input arguments.
     *
     * @param version log entry version
     * long as we still support those versions.
     */
    public static LogEntryVersion byVersion( byte version )
    {
        byte flattenedVersion = (byte) -version;

        if ( flattenedVersion < LOOKUP_BY_VERSION.length)
        {
            return LOOKUP_BY_VERSION[flattenedVersion];
        }
        throw new IllegalArgumentException( "Unrecognized log entry version " + version );
    }

    private static void put( LogEntryVersion[] array, int index, LogEntryVersion version )
    {
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
