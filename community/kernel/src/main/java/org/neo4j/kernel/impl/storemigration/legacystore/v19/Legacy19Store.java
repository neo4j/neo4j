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
package org.neo4j.kernel.impl.storemigration.legacystore.v19;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.neo4j.graphdb.Resource;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandWriter;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.LegacyRelationship20StoreReader;
import org.neo4j.kernel.impl.transaction.xaframework.DirectMappedLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.helpers.collection.ResourceClosingIterator.newResourceIterator;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.buildTypeDescriptorAndVersion;
import static org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1.writeLogHeader;

/**
 * Reader for a database in an older store format version.
 * <p/>
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the reader code is specific for the current upgrade and changes with each store format version.
 * <p/>
 * {@link #LEGACY_VERSION} marks which version it's able to read.
 */
public class Legacy19Store implements LegacyStore
{
    public static final String LEGACY_VERSION = "v0.A.0";

    private final File storageFileName;
    private final Collection<Closeable> allStoreReaders = new ArrayList<>();
    private Legacy19NodeStoreReader nodeStoreReader;
    private Legacy19PropertyIndexStoreReader propertyIndexReader;
    private LegacyPropertyStoreReader propertyStoreReader;
    private LegacyRelationshipStoreReader relStoreReader;

    private final FileSystemAbstraction fs;

    public Legacy19Store( FileSystemAbstraction fs, File storageFileName ) throws IOException
    {
        this.fs = fs;
        this.storageFileName = storageFileName;
        assertLegacyAndCurrentVersionHaveSameLength( LEGACY_VERSION, CommonAbstractStore.ALL_STORES_VERSION );
        initStorage();
    }

    /**
     * Store files that don't need migration are just copied and have their trailing versions replaced
     * by the current version. For this to work the legacy version and the current version must have the
     * same encoded length.
     */
    static void assertLegacyAndCurrentVersionHaveSameLength( String legacyVersion, String currentVersion )
    {
        if ( UTF8.encode( legacyVersion ).length != UTF8.encode( currentVersion ).length )
        {
            throw new IllegalStateException( "Encoded version string length must remain the same between versions" );
        }
    }

    private void initStorage() throws IOException
    {
        allStoreReaders.add( nodeStoreReader = new Legacy19NodeStoreReader(
                fs, new File( getStorageFileName().getPath() + StoreFactory.NODE_STORE_NAME ) ) );
        allStoreReaders.add( propertyIndexReader = new Legacy19PropertyIndexStoreReader(
                fs, new File( getStorageFileName().getPath() + StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME ) ) );
        allStoreReaders.add( propertyStoreReader = new LegacyPropertyStoreReader(
                fs, new File( getStorageFileName().getPath() + StoreFactory.PROPERTY_STORE_NAME ) ) );
        allStoreReaders.add( relStoreReader = new LegacyRelationship20StoreReader(
                fs, new File( getStorageFileName().getPath() + StoreFactory.RELATIONSHIP_STORE_NAME ) ) );
    }

    @Override
    public File getStorageFileName()
    {
        return storageFileName;
    }

    public static long getUnsignedInt( ByteBuffer buf )
    {
        return buf.getInt() & 0xFFFFFFFFL;
    }

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

    @Override
    public void close() throws IOException
    {
        for ( Closeable storeReader : allStoreReaders )
        {
            storeReader.close();
        }
    }

    private void copyStore( File targetBaseStorageFileName, String storeNamePart, String versionTrailer )
            throws IOException
    {
        File targetStoreFileName = new File( targetBaseStorageFileName.getPath() + storeNamePart );
        fs.copyFile( new File( storageFileName + storeNamePart ), targetStoreFileName );

        setStoreVersionTrailer( targetStoreFileName, versionTrailer );

        fs.copyFile(
                new File( storageFileName + storeNamePart + ".id" ),
                new File( targetBaseStorageFileName + storeNamePart + ".id" ) );
    }

    private void setStoreVersionTrailer( File targetStoreFileName, String versionTrailer ) throws IOException
    {
        try ( StoreChannel fileChannel = fs.open( targetStoreFileName, "rw" ) )
        {
            byte[] trailer = UTF8.encode( versionTrailer );
            fileChannel.position( fileChannel.size() - trailer.length );
            fileChannel.write( ByteBuffer.wrap( trailer ) );
        }
    }

    public void copyNeoStore( NeoStore neoStore ) throws IOException
    {
        copyStore( neoStore.getStorageFileName(), "", neoStore.getTypeAndVersionDescriptor() );
    }

    public void copyRelationshipStore( NeoStore neoStore ) throws IOException
    {
        copyStore( neoStore.getStorageFileName(), StoreFactory.RELATIONSHIP_STORE_NAME,
                buildTypeDescriptorAndVersion( RelationshipStore.TYPE_DESCRIPTOR ) );
    }

    public void copyRelationshipTypeTokenStore( NeoStore neoStore ) throws IOException
    {
        copyStore( neoStore.getStorageFileName(), StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME,
                buildTypeDescriptorAndVersion( RelationshipTypeTokenStore.TYPE_DESCRIPTOR ) );
    }

    public void copyRelationshipTypeTokenNameStore( NeoStore neoStore ) throws IOException
    {
        copyStore( neoStore.getStorageFileName(), StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME,
                buildTypeDescriptorAndVersion( DynamicStringStore.TYPE_DESCRIPTOR ) );
    }

    public void copyDynamicStringPropertyStore( NeoStore neoStore ) throws IOException
    {
        copyStore( neoStore.getStorageFileName(), StoreFactory.PROPERTY_STRINGS_STORE_NAME,
                buildTypeDescriptorAndVersion( DynamicStringStore.TYPE_DESCRIPTOR ) );
    }

    public void copyDynamicArrayPropertyStore( NeoStore neoStore ) throws IOException
    {
        copyStore( neoStore.getStorageFileName(), StoreFactory.PROPERTY_ARRAYS_STORE_NAME,
                buildTypeDescriptorAndVersion( DynamicArrayStore.TYPE_DESCRIPTOR ) );
    }

    @Override
    public LegacyNodeStoreReader getNodeStoreReader()
    {
        return nodeStoreReader;
    }

    @Override
    public LegacyRelationshipStoreReader getRelStoreReader()
    {
        return relStoreReader;
    }

    public Legacy19PropertyIndexStoreReader getPropertyIndexReader()
    {
        return propertyIndexReader;
    }

    public LegacyPropertyStoreReader getPropertyStoreReader()
    {
        return propertyStoreReader;
    }

    static void readIntoBuffer( StoreChannel fileChannel, ByteBuffer buffer, int nrOfBytes )
    {
        buffer.clear();
        buffer.limit( nrOfBytes );
        try
        {
            fileChannel.read( buffer );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        buffer.flip();
    }

    @Override
    public void copyLegacyIndexStoreFile( File toDirectory ) throws IOException
    {
        File legacyDirectory = storageFileName.getParentFile();
        File fromFile = new File( legacyDirectory, IndexStore.INDEX_DB_FILE_NAME );
        if ( fromFile.exists() )
        {
            File toFile = new File( toDirectory, IndexStore.INDEX_DB_FILE_NAME );
            fs.copyFile( fromFile, toFile );
        }
    }

    public void migrateTransactionLogs( FileSystemAbstraction fs, File migrationDir, File storeDir ) throws IOException
    {
        File[] logs = findAllTransactionLogs( storeDir );
        if ( logs == null )
        {
            return;
        }

        LogEntryWriterv1 logWriter = new LogEntryWriterv1();
        logWriter.setCommandWriter( new PhysicalLogNeoXaCommandWriter() );

        migrateLogs( fs, migrationDir, logs, logWriter, true );
    }

    public static void moveRewrittenTransactionLogs( File migrationDir, File storeDir )
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

        LogEntryWriterv1 logWriter = new LogEntryWriterv1();
        logWriter.setCommandWriter( LegacyLuceneCommandProcessor.newWriter() );

        migrateLogs( fs, migrationIndexDir, logs, logWriter, false );
    }

    public static void moveRewrittenLuceneLogs( File migrationDir, File storeDir )
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
                              LogEntryWriterv1 logWriter,
                              boolean transactionLogs ) throws IOException
    {
        for ( File legacyLogFile : logs )
        {
            File newLogFile = new File( migrationDir, legacyLogFile.getName() );
            try ( StoreChannel channel = fs.open( newLogFile, "rw" ) )
            {
                LogBuffer logBuffer = new DirectMappedLogBuffer( channel, ByteCounterMonitor.NULL );

                Iterator<LogEntry> legacyLogsIterator;
                if ( transactionLogs )
                {
                    legacyLogsIterator = iterateTransactionLogEntries( legacyLogFile, logBuffer );
                }
                else
                {
                    legacyLogsIterator = iterateLuceneLogEntries( legacyLogFile, logBuffer );
                }

                for ( LogEntry entry : loop( legacyLogsIterator ) )
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

    private Iterator<LogEntry> iterateTransactionLogEntries( File legacyLog, LogBuffer logBuffer ) throws IOException
    {
        return iterateLogEntries( legacyLog, logBuffer, new LegacyCommandReader() );
    }

    private Iterator<LogEntry> iterateLuceneLogEntries( File legacyLog, LogBuffer logBuffer ) throws IOException
    {
        return iterateLogEntries( legacyLog, logBuffer, LegacyLuceneCommandProcessor.newReader() );
    }

    private Iterator<LogEntry> iterateLogEntries( File legacyLogFile,
                                                  LogBuffer logBuffer,
                                                  LegacyLogIoUtil.CommandReader commandReader ) throws IOException
    {
        final StoreChannel channel = fs.open( legacyLogFile, "r" );
        final ByteBuffer buffer = ByteBuffer.allocate( 100000 );
        final LegacyLogIoUtil logIoUtil = new LegacyLogIoUtil( commandReader );

        long[] header = LegacyLogIoUtil.readLogHeader( buffer, channel, false );
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
