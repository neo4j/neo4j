/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV3_0_2;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.String.format;

/**
 * Main entry point into log entry versions and parsers and all that. A {@link LogEntryVersion} can be retrieved
 * by using {@link #byVersion(byte)} and from there get a hold of
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
 *   Starting with Neo4j version 2.1 a one-byte log version field was introduced with every single log entry.
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
 * the log entry version will be bumped.
 * The process of making an update to log entry or command format is to:
 * <ol>
 * <li>Copy {@link PhysicalLogCommandReaderV3_0_2} or similar and modify the new copy</li>
 * <li>Copy {@link LogEntryParsersV2_3} or similar and modify the new copy if entry layout has changed</li>
 * <li>Add an entry in this enum, like {@link #V3_0_10} pointing to the above new classes, version needs to be negative
 * to detect log files from older versions of neo4j</li>
 * <li>Modify {@link StorageCommand#serialize(WritableChannel)}.
 * Also {@link LogEntryWriter} (if log entry layout has changed) with required changes</li>
 * <li>Change {@link #CURRENT} to point to the newly created version</li>
 * </ol>
 * Everything apart from that should just work and Neo4j should automatically support the new version as well.
 */
public enum LogEntryVersion
{
    V2_3( -5, LogEntryParsersV2_3.class ),
    V3_0( -6, LogEntryParsersV2_3.class ),
    // as of 2016-05-30: neo4j 2.3.5 explicit index IndexDefineCommand maps write size as short instead of byte
    // log entry layout hasn't changed since 2_3 so just use that one
    V2_3_5( -8, LogEntryParsersV2_3.class ),
    // as of 2016-05-30: neo4j 3.0.2 explicit index IndexDefineCommand maps write size as short instead of byte
    // log entry layout hasn't changed since 2_3 so just use that one
    V3_0_2( -9, LogEntryParsersV2_3.class ),
    // as of 2017-05-26: the records in command log entries include a bit that specifies if the command is serialised
    // using a fixed-width reference format, or not. This change is technically backwards compatible, so we bump the
    // log version to prevent mixed-version clusters from forming.
    V3_0_10( -10, LogEntryParsersV2_3.class );
    // Method moreRecentVersionExists() relies on the fact that we have negative numbers, thus next version to use is -11

    public static final LogEntryVersion CURRENT = V3_0_10;
    private static final byte LOWEST_VERSION = (byte)-V2_3.byteCode();
    private static final LogEntryVersion[] ALL = values();
    private static final LogEntryVersion[] LOOKUP_BY_VERSION;
    static
    {
        LOOKUP_BY_VERSION = new LogEntryVersion[(-CURRENT.byteCode()) + 1]; // pessimistic size
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
     * Check if a more recent version of the log entry format exists and can be handled.
     *
     * @param version to compare against latest version
     * @return {@code true} if a more recent log entry version exists
     */
    public static boolean moreRecentVersionExists( LogEntryVersion version )
    {
        return version.version > CURRENT.version; // reverted do to negative version numbers
    }

    /**
     * Return the correct {@link LogEntryVersion} for the given {@code version} code read from e.g. a log entry.
     * Lookup is fast and can be made inside critical paths, no need for externally caching the returned
     * {@link LogEntryVersion} instance per the input arguments.
     *
     * @param version log entry version
     */
    public static LogEntryVersion byVersion( byte version )
    {
        byte positiveVersion = (byte) -version;

        if ( positiveVersion >= LOWEST_VERSION && positiveVersion < LOOKUP_BY_VERSION.length )
        {
            return LOOKUP_BY_VERSION[positiveVersion];
        }
        byte positiveCurrentVersion = (byte) -CURRENT.byteCode();
        if ( positiveVersion > positiveCurrentVersion )
        {
            throw new IllegalArgumentException( String.format(
                    "Transaction logs contains entries with prefix %d, and the highest supported prefix is %d. This " +
                            "indicates that the log files originates from a newer version of neo4j.",
                    positiveVersion, positiveCurrentVersion ) );
        }
        throw new IllegalArgumentException( String.format(
                "Transaction logs contains entries with prefix %d, and the lowest supported prefix is %d. This " +
                        "indicates that the log files originates from an older version of neo4j, which we don't support " +
                        "migrations from.",
                positiveVersion, LOWEST_VERSION ) );
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
