/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
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
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;

import static org.neo4j.helpers.collection.ResourceClosingIterator.newResourceIterator;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.buildTypeDescriptorAndVersion;

/**
 * Reader for a database in an older store format version.
 *
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the reader code is specific for the current upgrade and changes with each store format version.
 *
 * {@link #LEGACY_VERSION} marks which version it's able to read.
 */
public class LegacyStore implements Closeable
{
    public static final String LEGACY_VERSION = "v0.A.0";

    // We are going to blindly assume that people never configure their
    // transaction log filenames off the default. If they do, then they're
    // in trouble.
    private static final String NIONEO_LOGICAL_LOG_PATTERN = "nioneo_logical\\.log\\.v.*";
    private static final String LUCENE_LOGICAL_LOG_PATTERN = "lucene\\.log\\.v.*";

    private final File storageFileName;
    private final Collection<Closeable> allStoreReaders = new ArrayList<>();
    private LegacyNodeStoreReader nodeStoreReader;
    private LegacyPropertyIndexStoreReader propertyIndexReader;
    private LegacyPropertyStoreReader propertyStoreReader;

    private final FileSystemAbstraction fs;

    public LegacyStore( FileSystemAbstraction fs, File storageFileName ) throws IOException
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

    protected void initStorage() throws IOException
    {
        allStoreReaders.add( nodeStoreReader = new LegacyNodeStoreReader( fs, new File( getStorageFileName().getPath() + StoreFactory.NODE_STORE_NAME ) ) );
        allStoreReaders.add( propertyIndexReader = new LegacyPropertyIndexStoreReader( fs, new File( getStorageFileName().getPath() + StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME ) ) );
        allStoreReaders.add( propertyStoreReader = new LegacyPropertyStoreReader( fs, new File( getStorageFileName().getPath() + StoreFactory.PROPERTY_STORE_NAME ) ) );
    }

    public File getStorageFileName()
    {
        return storageFileName;
    }

    public static long getUnsignedInt(ByteBuffer buf)
    {
        return buf.getInt()&0xFFFFFFFFL;
    }

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base|modifier;
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

    public LegacyNodeStoreReader getNodeStoreReader()
    {
        return nodeStoreReader;
    }

    public LegacyPropertyIndexStoreReader getPropertyIndexReader()
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

    /**
     * Prepare a StoreChannel for writing a translated version of the last transaction log
     * to the correct location, or return null if the legacy store has no transaction logs
     * that can be translated.
     */
    public StoreChannel beginTranslatingLastTransactionLog( NeoStore neoStore ) throws IOException
    {
        File legacyDirectory = storageFileName.getParentFile();
        File migratedDirectory = neoStore.getStorageFileName().getParentFile();
        String logFilenamePattern = NIONEO_LOGICAL_LOG_PATTERN;

        return prepareLogTranslation( legacyDirectory, migratedDirectory, logFilenamePattern );
    }

    /**
     * Prepare a StoreChannel for writing a translated version of the last legacy lucene index
     * log to the correct location, or return null if the legacy store has no legacy lucene
     * index transaction logs that can be translated.
     */
    public StoreChannel beginTranslatingLastLuceneLog( NeoStore neoStore ) throws IOException
    {
        File legacyDirectory = new File( storageFileName.getParentFile(), "index" );
        File migratedDirectory = new File( neoStore.getStorageFileName().getParentFile(), "index" );
        fs.mkdirs( migratedDirectory );
        String logFilenamePattern = LUCENE_LOGICAL_LOG_PATTERN;

        return prepareLogTranslation( legacyDirectory, migratedDirectory, logFilenamePattern );
    }

    private StoreChannel prepareLogTranslation(
            File legacyDirectory,
            File migratedDirectory,
            String logFilenamePattern ) throws IOException
    {
        File lastLegacyLog = findMostRecentLog( legacyDirectory, logFilenamePattern );
        if ( lastLegacyLog == null )
        {
            return null;
        }
        File translatedLogFile = new File( migratedDirectory, lastLegacyLog.getName() );
        return fs.open( translatedLogFile, "rw" );
    }

    private File findMostRecentLog( File directory, String logFilenamePattern )
    {
        final Pattern logFileName = Pattern.compile( logFilenamePattern );
        FilenameFilter logFiles = new FilenameFilter()
        {
            @Override
            public boolean accept( File dir, String name )
            {
                return logFileName.matcher( name ).find();
            }
        };

        File[] files = fs.listFiles( directory, logFiles );
        // 'files' will be 'null' if an IO error occurs.
        if ( files != null && files.length > 0 )
        {
            Arrays.sort( files );
            return files[ files.length - 1 ];
        }
        return null;
    }

    public ResourceIterator<LogEntry> iterateLastTransactionLogEntries(
            LogBuffer logBuffer ) throws IOException
    {
        File legacyDirectory = storageFileName.getParentFile();
        String logFilenamePattern = NIONEO_LOGICAL_LOG_PATTERN;
        final LegacyLogCommandReader commandReader = new LegacyLogicalLogCommandReader();

        return iterateLogEntries( logBuffer, legacyDirectory, logFilenamePattern, commandReader );
    }

    public ResourceIterator<LogEntry> iterateLastLuceneLogEntries(
            LogBuffer logBuffer ) throws IOException
    {
        File legacyDirectory = new File( storageFileName.getParentFile(), "index" );
        String logFilenamePattern = LUCENE_LOGICAL_LOG_PATTERN;
        final LegacyLogCommandReader commandReader = new LegacyLuceneLogCommandReader();

        return iterateLogEntries( logBuffer, legacyDirectory, logFilenamePattern, commandReader );
    }

    private ResourceIterator<LogEntry> iterateLogEntries(
            LogBuffer logBuffer,
            File legacyDirectory,
            String logFilenamePattern,
            final LegacyLogCommandReader commandReader ) throws IOException
    {
        File lastLegacyLog = findMostRecentLog( legacyDirectory, logFilenamePattern );
        final StoreChannel channel = fs.open( lastLegacyLog, "r" );
        final ByteBuffer buffer = ByteBuffer.allocate( 100000 );

        long[] header = LegacyLogIoUtil.readLogHeader( buffer, channel, false );
        if ( header != null )
        {
            ByteBuffer headerBuf = ByteBuffer.allocate( 16 );
            LogIoUtils.writeLogHeader( headerBuf, header[0], header[1] );
            logBuffer.put( headerBuf.array() );
        }

        Resource resource = channelAsResource( channel );
        return newResourceIterator( resource, new PrefetchingIterator<LogEntry>()
        {
            @Override
            protected LogEntry fetchNextOrNull()
            {
                try
                {
                    return LegacyLogIoUtil.readEntry( buffer, channel, commandReader );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( "Failed to read legacy log entry", e );
                }
            }
        } );
    }

    private Resource channelAsResource( final StoreChannel channel )
    {
        return new Resource()
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
    }
}
