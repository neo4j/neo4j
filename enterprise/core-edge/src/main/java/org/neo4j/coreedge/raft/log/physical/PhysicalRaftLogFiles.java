/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.log.physical;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.lang.Math.max;
import static java.lang.Math.min;

import static java.lang.String.format;
import static org.neo4j.coreedge.raft.log.physical.PhysicalRaftLogFile.openForVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;

/**
 * Used to figure out what logical log file to open when the database
 * starts up.
 */
public class PhysicalRaftLogFiles extends LifecycleAdapter
{
    private final File logBaseName;
    private final Pattern logFilePattern;

    private final FileSystemAbstraction fileSystem;
    private final ChannelMarshal<ReplicatedContent> marshal;
    private final VersionIndexRanges versionRanges;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate( LOG_HEADER_SIZE );

    public static final String BASE_FILE_NAME = "raft.log";
    public static final String REGEX_DEFAULT_VERSION_SUFFIX = "\\.";
    public static final String DEFAULT_VERSION_SUFFIX = ".";

    public PhysicalRaftLogFiles(
            File directory, FileSystemAbstraction fileSystem, ChannelMarshal<ReplicatedContent> marshal,
            VersionIndexRanges versionRanges )
    {
        this.marshal = marshal;
        this.logBaseName = new File( directory, BASE_FILE_NAME );
        this.logFilePattern = Pattern.compile( BASE_FILE_NAME + REGEX_DEFAULT_VERSION_SUFFIX + "\\d+" );
        this.fileSystem = fileSystem;
        this.versionRanges = versionRanges;
    }

    @Override
    public void init() throws IOException
    {
        long lowest = -1;
        long highest = -1;

        SortedSet<Long> versions = new TreeSet<>();

        for ( File file : fileSystem.listFiles( logBaseName.getParentFile() ) )
        {
            if ( logFilePattern.matcher( file.getName() ).matches() )
            {
                // Get version based on the name
                long logVersion = getLogVersion( file.getName() );
                versions.add( logVersion );
                highest = max( highest, logVersion );
                lowest = lowest == -1 ? logVersion : min( lowest, logVersion );
            }
        }
        long appendIndex = -1;
        Long previousVersion = null;
        for ( Long version : versions )
        {
            File file = getLogFileForVersion( version );
            LogHeader logHeader = readLogHeader( fileSystem, file, false );
            if ( logHeader == null )
            {
                if ( previousVersion != null )
                {
                    appendIndex = readLastIndex( previousVersion, appendIndex );
                }
                writeHeader( file, version, appendIndex );
            }
            else
            {
                appendIndex = logHeader.lastCommittedTxId;
            }
            versionRanges.add( version, appendIndex );
            previousVersion = version;
        }
    }

    private void writeHeader( File file, long version, long prevIndex ) throws IOException
    {
        // Either the header is not there in full or the file was new. Don't care
        StoreChannel storeChannel = fileSystem.open( file, "rw" );
        writeLogHeader( headerBuffer, version, prevIndex );
        storeChannel.writeAll( headerBuffer );
    }

    private long readLastIndex( Long version, long previousAppendIndex ) throws IOException
    {
        long lastIndex = previousAppendIndex;
        PhysicalLogVersionedStoreChannel logChannel = openForVersion( this, fileSystem, version );
        try ( RaftRecordCursor<ReadAheadLogChannel> cursor = new RaftRecordCursor<>( new
                ReadAheadLogChannel( logChannel, LogVersionBridge.NO_MORE_CHANNELS ), marshal ))
        {
            while ( cursor.next() )
            {
                RaftLogRecord raftLogRecord = cursor.get();
                if ( raftLogRecord.getType() == PhysicalRaftLog.RecordType.APPEND )
                {
                    lastIndex = ((RaftLogAppendRecord) raftLogRecord).logIndex();
                }
            }
        }

        return lastIndex;
    }

    public File getLogFileForVersion( long version )
    {
        return new File( logBaseName.getPath() + DEFAULT_VERSION_SUFFIX + version );
    }

    public boolean versionExists( long version )
    {
        return fileSystem.fileExists( getLogFileForVersion( version ) );
    }

    public LogHeader extractHeader( long version ) throws IOException
    {
        return readLogHeader( fileSystem, getLogFileForVersion( version ) );
    }

    public boolean hasAnyEntries( long version )
    {
        return fileSystem.getFileSize( getLogFileForVersion( version ) ) > LOG_HEADER_SIZE;
    }

    public long getHighestLogVersion()
    {
        return versionRanges.highestVersion();
    }

    public long getLowestLogVersion()
    {
        return versionRanges.lowestVersion();
    }

    static long getLogVersion( String historyLogFilename )
    {
        int index = historyLogFilename.lastIndexOf( DEFAULT_VERSION_SUFFIX );
        if ( index == -1 )
        {
            throw new RuntimeException( "Invalid log file '" + historyLogFilename + "'" );
        }
        return Long.parseLong( historyLogFilename.substring( index + DEFAULT_VERSION_SUFFIX.length() ) );
    }

    public void createdFile( long forVersion, long prevEntryIndex )
    {
        versionRanges.add( forVersion, prevEntryIndex );
    }

    public boolean containsEntries( long version )
    {
        return fileSystem.getFileSize( getLogFileForVersion( version ) ) > LOG_HEADER_SIZE;
    }

    public void pruneUpTo( long upper )
    {
        // Find out which log is the earliest existing (lower bound to prune)
        long lower = getLowestLogVersion();

        // The reason we delete from lower to upper is that if it crashes in the middle
        // we can be sure that no holes are created
        for ( long version = lower; version <= upper; version++ )
        {
            long lowestLogVersion = getLowestLogVersion();
            fileSystem.deleteFile( getLogFileForVersion( version ) );
            versionRanges.pruneVersion( version );
            assert version >= lowestLogVersion :
                    format( "Cannot prune to log version %d lower than the lowest existing %d",
                            version, lowestLogVersion );
        }
    }

    public long registerNextVersion( Long prevLogIndex )
    {
        long version = getHighestLogVersion() + 1;
        versionRanges.add( version, prevLogIndex );
        return version;
    }
}
