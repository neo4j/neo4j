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
package org.neo4j.kernel.impl.storemigration.legacystore;

import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.buildTypeDescriptorAndVersion;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.StoreFile;

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
    public static final String LEGACY_VERSION = "v0.A.1";

    private final File storageFileName;
    private final Collection<Closeable> allStoreReaders = new ArrayList<>();
    private LegacyNodeStoreReader nodeStoreReader;
    private LegacyRelationshipStoreReader relStoreReader;

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
        allStoreReaders.add( nodeStoreReader = new LegacyNodeStoreReader( fs,
                new File( getStorageFileName().getPath() + StoreFactory.NODE_STORE_NAME ) ) );
        allStoreReaders.add( relStoreReader = new LegacyRelationshipStoreReader( fs,
                new File( getStorageFileName().getPath() + StoreFactory.RELATIONSHIP_STORE_NAME ) ) );
    }

    public File getStorageFileName()
    {
        return storageFileName;
    }

    public static long getUnsignedInt( ByteBuffer buf )
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

    public static void ensureStoreVersionTrailer( FileSystemAbstraction fs,
            File storeDir, Iterable<StoreFile> files ) throws IOException
    {
        for ( StoreFile file : files )
        {
            setStoreVersionTrailer( fs, new File( storeDir, file.storeFileName() ),
                    buildTypeDescriptorAndVersion( file.typeDescriptor() ) );
        }
    }

    public static void setStoreVersionTrailer( FileSystemAbstraction fs,
            File targetStoreFileName, String versionTrailer ) throws IOException
    {
        byte[] trailer = UTF8.encode( versionTrailer );
        long fileSize = 0;
        try ( StoreChannel fileChannel = fs.open( targetStoreFileName, "rw" ) )
        {
            fileSize = fileChannel.size();
            fileChannel.position( fileChannel.size() - trailer.length );
            fileChannel.write( ByteBuffer.wrap( trailer ) );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IllegalArgumentException( "size:" + fileSize + ", trailer:" + trailer.length +
                    " for " + targetStoreFileName );
        }
    }

    public LegacyNodeStoreReader getNodeStoreReader()
    {
        return nodeStoreReader;
    }

    public LegacyRelationshipStoreReader getRelStoreReader()
    {
        return relStoreReader;
    }

    static void readIntoBuffer( StoreChannel fileChannel, ByteBuffer buffer, long atPosition, int nrOfBytes )
    {
        try
        {
            fileChannel.position( atPosition );
            readIntoBuffer( fileChannel, buffer, nrOfBytes );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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
}
