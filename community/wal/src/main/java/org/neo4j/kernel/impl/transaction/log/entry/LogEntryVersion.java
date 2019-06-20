/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.eclipse.collections.api.set.primitive.MutableByteSet;
import org.eclipse.collections.impl.map.mutable.primitive.ByteObjectHashMap;

import org.neo4j.io.fs.WritableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * Main entry point into log entry versions and parsers. A {@link LogEntryVersion} can be retrieved
 * by using {@link #byVersion(byte)} and from there get a hold of
 * {@link LogEntryParser} using {@link #entryParser(byte)}.
 * <p/>
 * Transactions are written into a log. Each transaction consists of one or more log entries. Log entries
 * can be of one or more types, such as denoting start of a transaction, commands and committing the transaction.
 * Neo4j supports writing the latest/current log entry and reading log entries for all currently supported versions.
 * Current versioning scheme uses one-byte log version field in every single log entry.
 * <p/>
 * As of 2.2.4 the log-global format version is gone, although still just a token value written to adhere to
 * the 16 bytes header size of a log for backwards compatibility. The log entry version controls everything
 * about versioning of log entries and commands, such that if either log entry format (such as log entry types,
 * such as START, COMMIT and the likes, or data within them) change, or one or more command format change
 * the log entry version will be bumped.
 * <p/>
 * The process of making an update to log entry or command format is to:
 * <ol>
 * <li>Copy the latest {@link CommandReader} or similar and modify the new copy</li>
 * <li>Copy {@link LogEntryParsersV2_3} or similar and modify the new copy if entry layout has changed</li>
 * <li>Add an entry in this enum, like {@link #V3_0_10} pointing to the above new classes</li>
 * <li>Modify {@link StorageCommand#serialize(WritableChannel)}.
 * Also LogEntryWriter (if log entry layout has changed) with required changes</li>
 * </ol>
 */
public enum LogEntryVersion
{
    // as of 2017-05-26: the records in command log entries include a bit that specifies if the command is serialised
    // using a fixed-width reference format, or not. This change is technically backwards compatible, so we bump the
    // log version to prevent mixed-version clusters from forming.
    V3_0_10( (byte) -10, LogEntryParsersV2_3.class ),
    // Version 4.0 introduces a new schema store format, where the schema store payload is stored in the property store.
    // This adds a new schema record command.
    // We bump the log version to prevent older releases from thinking they can read log files with these new commands.
    V4_0( (byte) 1, LogEntryParsersV2_3.class );

    public static final LogEntryVersion LATEST_VERSION;
    private static final byte LOWEST_VERSION;
    private static final LogEntryVersion[] ALL = values();
    private static final ByteObjectHashMap<LogEntryVersion> LOOKUP_BY_VERSION;

    static
    {
        LOOKUP_BY_VERSION = new ByteObjectHashMap<>();
        for ( LogEntryVersion version : ALL )
        {
            LOOKUP_BY_VERSION.put( version.version(), version );
        }
        MutableByteSet keys = LOOKUP_BY_VERSION.keySet();
        LOWEST_VERSION = keys.min();
        LATEST_VERSION = LOOKUP_BY_VERSION.get( keys.max() );
    }

    private final byte version;
    private final LogEntryParser<LogEntry>[] entryTypes;

    LogEntryVersion( byte version, Class<? extends Enum<? extends LogEntryParser<? extends LogEntry>>> cls )
    {
        this.entryTypes = new LogEntryParser[highestCode( cls ) + 1];
        for ( Enum<? extends LogEntryParser<? extends LogEntry>> parser : cls.getEnumConstants() )
        {
            LogEntryParser<LogEntry> candidate = (LogEntryParser<LogEntry>) parser;
            this.entryTypes[candidate.byteCode()] = candidate;
        }
        this.version = version;
    }

    /**
     * @return byte value representing this log entry version.
     */
    public byte version()
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
        return version.version < LATEST_VERSION.version;
    }

    /**
     * Return the correct {@link LogEntryVersion} for the given {@code version} code read from e.g. a log entry.
     *
     * @param version log entry version
     */
    public static LogEntryVersion byVersion( byte version )
    {
        LogEntryVersion logEntryVersion = LOOKUP_BY_VERSION.get( version );
        if ( logEntryVersion != null )
        {
            return logEntryVersion;
        }
        byte latestVersion = LATEST_VERSION.version();
        if ( version > latestVersion )
        {
            throw new UnsupportedLogVersionException( String.format(
                    "Transaction logs contains entries with prefix %d, and the highest supported prefix is %d. This " +
                            "indicates that the log files originates from a newer version of neo4j.",
                    version, latestVersion ) );
        }
        throw new UnsupportedLogVersionException( String.format(
                "Transaction logs contains entries with prefix %d, and the lowest supported prefix is %d. This " +
                        "indicates that the log files originates from an older version of neo4j, which we don't support " +
                        "migrations from.",
                version, LOWEST_VERSION ) );
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
