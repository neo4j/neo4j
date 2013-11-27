/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;

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

    private final File storageFileName;
    private final Collection<Closeable> allStoreReaders = new ArrayList<Closeable>();
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
            throw new IllegalStateException( "Encoded version string length must remain the same between versions" );
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
            storeReader.close();
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
        FileChannel fileChannel = fs.open( targetStoreFileName, "rw" );
        try
        {
            byte[] trailer = UTF8.encode( versionTrailer );
            fileChannel.position( fileChannel.size()-trailer.length );
            fileChannel.write( ByteBuffer.wrap( trailer ) );
        }
        finally
        {
            fileChannel.close();
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

    public void copyPropertyStore( NeoStore neoStore ) throws IOException
    {
        copyStore( neoStore.getStorageFileName(), StoreFactory.PROPERTY_STORE_NAME,
                buildTypeDescriptorAndVersion( PropertyStore.TYPE_DESCRIPTOR ) );
    }

    public void copyPropertyKeyTokenStore( NeoStore neoStore ) throws IOException
    {
        copyStore( neoStore.getStorageFileName(), StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME,
                buildTypeDescriptorAndVersion( PropertyKeyTokenStore.TYPE_DESCRIPTOR ) );
    }

    public void copyPropertyKeyTokenNameStore( NeoStore neoStore ) throws IOException
    {
        copyStore( neoStore.getStorageFileName(), StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME,
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
    
    static void readIntoBuffer( FileChannel fileChannel, ByteBuffer buffer, int nrOfBytes )
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
}
