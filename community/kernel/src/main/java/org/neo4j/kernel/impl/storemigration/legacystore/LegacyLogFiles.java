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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.neo4j.graphdb.Resource;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.transaction.xaframework.DirectMappedLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriter;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.helpers.collection.ResourceClosingIterator.newResourceIterator;
import static org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1.writeLogHeader;

public class LegacyLogFiles
{
    private final FileSystemAbstraction fs;
    private final LogEntryWriter logEntryWriter;
    private final LogEntryWriter luceneLogEntryWriter;
    private final LegacyLogIoUtil logIoUtil;
    private final LegacyLogIoUtil luceneLogIoUtil;

    public LegacyLogFiles( FileSystemAbstraction fs,
                           LogEntryWriter logEntryWriter,
                           LogEntryWriter luceneLogEntryWriter,
                           LegacyLogIoUtil logIoUtil,
                           LegacyLogIoUtil luceneLogIoUtil )
    {
        this.fs = fs;
        this.logEntryWriter = logEntryWriter;
        this.luceneLogEntryWriter = luceneLogEntryWriter;
        this.logIoUtil = logIoUtil;
        this.luceneLogIoUtil = luceneLogIoUtil;
    }

    public void migrateNeoLogs( FileSystemAbstraction fs, File migrationDir, File storeDir ) throws IOException
    {
        File[] logs = findAllTransactionLogs( storeDir );
        if ( logs == null )
        {
            return;
        }

        migrateLogs( fs, migrationDir, logs, logEntryWriter, logIoUtil );
    }

    public void moveRewrittenNeoLogs( File migrationDir, File storeDir )
    {
        File[] transactionLogs = findAllTransactionLogs( migrationDir );
        if ( transactionLogs != null )
        {
            for ( File rewrittenLog : transactionLogs )
            {
                moveRewrittenLogFile( rewrittenLog, storeDir );
            }
        }
    }

    public void migrateLuceneLogs( FileSystemAbstraction fs, File migrationDir, File storeDir ) throws IOException
    {
        File[] logs = findAllLuceneLogs( storeDir );
        if ( logs == null )
        {
            return;
        }

        File migrationIndexDir = indexDirIn( migrationDir );
        fs.mkdir( migrationIndexDir );

        migrateLogs( fs, migrationIndexDir, logs, luceneLogEntryWriter, luceneLogIoUtil );
    }

    public void moveRewrittenLuceneLogs( File migrationDir, File storeDir )
    {
        File[] luceneLogs = findAllLuceneLogs( migrationDir );
        if ( luceneLogs != null )
        {
            File indexDir = indexDirIn( storeDir );
            for ( File rewrittenLog : luceneLogs )
            {
                moveRewrittenLogFile( rewrittenLog, indexDir );
            }
        }
    }

    private void migrateLogs( FileSystemAbstraction fs,
                              File migrationDir,
                              File[] logs,
                              LogEntryWriter logWriter,
                              LegacyLogIoUtil logIoUtil ) throws IOException
    {
        for ( File legacyLogFile : logs )
        {
            File newLogFile = new File( migrationDir, legacyLogFile.getName() );
            try ( StoreChannel channel = fs.open( newLogFile, "rw" ) )
            {
                LogBuffer logBuffer = new DirectMappedLogBuffer( channel, ByteCounterMonitor.NULL );
                Iterator<LogEntry> iterator = iterateLogEntries( legacyLogFile, logBuffer, logIoUtil );
                for ( LogEntry entry : loop( iterator ) )
                {
                    logWriter.writeLogEntry( entry, logBuffer );
                }

                logBuffer.force();
                channel.close();
            }
        }
    }

    private static File[] findAllTransactionLogs( File dir )
    {
        return findAllLogs( dir, "nioneo_logical\\.log\\.v.*" );
    }

    private static File[] findAllLuceneLogs( File dir )
    {
        return findAllLogs( indexDirIn( dir ), "lucene\\.log\\.v.*" );
    }

    private static File[] findAllLogs( File dir, String namePattern )
    {
        final Pattern logFileName = Pattern.compile( namePattern );
        FilenameFilter logFiles = new FilenameFilter()
        {
            @Override
            public boolean accept( File dir, String name )
            {
                return logFileName.matcher( name ).find();
            }
        };
        File[] files = dir.listFiles( logFiles );
        // 'files' will be 'null' if an IO error occurs.
        if ( files != null && files.length > 0 )
        {
            Arrays.sort( files );
        }
        return files;
    }

    private Iterator<LogEntry> iterateLogEntries( File legacyLogFile,
                                                  LogBuffer logBuffer,
                                                  final LegacyLogIoUtil logIoUtil ) throws IOException
    {
        final StoreChannel channel = fs.open( legacyLogFile, "r" );
        final ByteBuffer buffer = ByteBuffer.allocate( 100000 );

        long[] header = logIoUtil.readLogHeader( buffer, channel, false );
        if ( header != null )
        {
            ByteBuffer headerBuf = ByteBuffer.allocate( 16 );
            writeLogHeader( headerBuf, header[0], header[1] );
            logBuffer.put( headerBuf.array() );
        }

        Resource resource = new Resource()
        {
            @Override
            public void close()
            {
                try
                {
                    channel.close();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "Failed to close legacy log channel", e );
                }
            }
        };
        return newResourceIterator( resource, new PrefetchingIterator<LogEntry>()
        {
            @Override
            protected LogEntry fetchNextOrNull()
            {
                try
                {
                    return logIoUtil.readEntry( buffer, channel );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "Failed to read legacy log entry", e );
                }
            }
        } );
    }

    private static File indexDirIn( File baseDir )
    {
        return new File( baseDir, "index" );
    }

    private static void moveRewrittenLogFile( File newLogFile, File storeDir )
    {
        Path oldLogFile = Paths.get( storeDir.getAbsolutePath(), newLogFile.getName() );
        try
        {
            Files.move( newLogFile.toPath(), oldLogFile, REPLACE_EXISTING );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to move rewritten log to store dir", e );
        }
    }
}
