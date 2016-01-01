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
package org.neo4j.kernel.impl.storemigration.legacystore.v20;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.StoreFileType;

import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.buildTypeDescriptorAndVersion;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.setStoreVersion;
import static org.neo4j.kernel.impl.nioneo.store.NeoStore.versionStringToLong;

public enum StoreFile20
{
    NODE_STORE( NodeStore.TYPE_DESCRIPTOR, StoreFactory.NODE_STORE_NAME ),
    NODE_LABEL_STORE( DynamicArrayStore.TYPE_DESCRIPTOR, StoreFactory.NODE_LABELS_STORE_NAME ),
    PROPERTY_STORE( PropertyStore.TYPE_DESCRIPTOR, StoreFactory.PROPERTY_STORE_NAME ),
    PROPERTY_ARRAY_STORE( DynamicArrayStore.TYPE_DESCRIPTOR, StoreFactory.PROPERTY_ARRAYS_STORE_NAME ),
    PROPERTY_STRING_STORE( DynamicStringStore.TYPE_DESCRIPTOR, StoreFactory.PROPERTY_STRINGS_STORE_NAME ),
    PROPERTY_KEY_TOKEN_STORE( PropertyKeyTokenStore.TYPE_DESCRIPTOR, StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME ),
    PROPERTY_KEY_TOKEN_NAMES_STORE( DynamicStringStore.TYPE_DESCRIPTOR, StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),
    RELATIONSHIP_STORE( RelationshipStore.TYPE_DESCRIPTOR, StoreFactory.RELATIONSHIP_STORE_NAME ),
    RELATIONSHIP_TYPE_TOKEN_STORE( RelationshipTypeTokenStore.TYPE_DESCRIPTOR, StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME ),
    RELATIONSHIP_TYPE_TOKEN_NAMES_STORE( DynamicStringStore.TYPE_DESCRIPTOR, StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),
    LABEL_TOKEN_STORE( LabelTokenStore.TYPE_DESCRIPTOR, StoreFactory.LABEL_TOKEN_STORE_NAME ),
    LABEL_TOKEN_NAMES_STORE( DynamicStringStore.TYPE_DESCRIPTOR, StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME ),
    SCHEMA_STORE( SchemaStore.TYPE_DESCRIPTOR, StoreFactory.SCHEMA_STORE_NAME ),
    RELATIONSHIP_GROUP_STORE( RelationshipGroupStore.TYPE_DESCRIPTOR, StoreFactory.RELATIONSHIP_GROUP_STORE_NAME, false ),
    NEO_STORE( NeoStore.TYPE_DESCRIPTOR, "" );

    private final String typeDescriptor;
    private final String storeFileNamePart;
    private final boolean existsInBoth;

    private StoreFile20( String typeDescriptor, String storeFileNamePart )
    {
        this( typeDescriptor, storeFileNamePart, true );
    }

    private StoreFile20( String typeDescriptor, String storeFileNamePart, boolean existsInBoth )
    {
        this.typeDescriptor = typeDescriptor;
        this.storeFileNamePart = storeFileNamePart;
        this.existsInBoth = existsInBoth;
    }

    public String legacyVersion()
    {
        return typeDescriptor + " " + Legacy20Store.LEGACY_VERSION;
    }

    /**
     * The first part of the version String.
     */
    public String typeDescriptor()
    {
        return typeDescriptor;
    }

    public String fileName( StoreFileType type )
    {
        return type.augment( NeoStore.DEFAULT_NAME + storeFileNamePart );
    }

    public String storeFileName()
    {
        return fileName( StoreFileType.STORE );
    }

    public String idFileName()
    {
        return fileName( StoreFileType.ID );
    }

    public static Iterable<StoreFile20> legacyStoreFiles()
    {
        Predicate<StoreFile20> predicate = new Predicate<StoreFile20>()
        {
            @Override
            public boolean accept( StoreFile20 item )
            {
                return item.existsInBoth;
            }
        };
        Iterable<StoreFile20> storeFiles = currentStoreFiles();
        return Iterables.filter( predicate, storeFiles );
    }

    public static Iterable<StoreFile20> currentStoreFiles()
    {
        return Iterables.iterable( values() );
    }

    /**
     * Moves a database's store files from one directory
     * to another. Since it just renames files (the standard way of moving with
     * JDK6) from and to must be on the same disk partition.
     *
     * @param fromDirectory The directory that hosts the database files.
     * @param toDirectory The directory to move the database files to.
     * @throws IOException If any of the move operations fail for any reason.
     */
    public static void move( FileSystemAbstraction fs, File fromDirectory, File toDirectory,
            Iterable<StoreFile20> files, boolean allowSkipNonExistentFiles, boolean allowOverwriteTarget,
            StoreFileType... types ) throws IOException
    {
        // TODO: change the order that files are moved to handle failure conditions properly
        for ( StoreFile20 storeFile : files )
        {
            for ( StoreFileType type : types )
            {
                moveFile( fs, storeFile.fileName( type ), fromDirectory, toDirectory,
                        allowSkipNonExistentFiles, allowOverwriteTarget );
            }
        }
    }

    /**
     * Moves a file from one directory to another, by a rename op.
     * @param fs
     *
     * @param fileName The base filename of the file to move, not the complete
     *            path
     * @param fromDirectory The directory currently containing filename
     * @param toDirectory The directory to host filename - must be in the same
     *            disk partition as filename
     * @param allowOverwriteTarget
     * @throws IOException
     */
    public static void moveFile( FileSystemAbstraction fs, String fileName, File fromDirectory,
            File toDirectory, boolean allowSkipNonExistentFiles, boolean allowOverwriteTarget ) throws IOException
    {
        File sourceFile = new File( fromDirectory, fileName );
        if ( allowSkipNonExistentFiles && !fs.fileExists( sourceFile ) )
        {   // The source file doesn't exist and we allow skipping, so return
            return;
        }

        File toFile = new File( toDirectory, fileName );
        if ( allowOverwriteTarget && fs.fileExists( toFile ) )
        {
            fs.deleteFile( toFile );
        }

        fs.moveToDirectory( sourceFile, toDirectory );
    }

    public static void ensureStoreVersion( FileSystemAbstraction fs,
            File storeDir, Iterable<StoreFile20> files ) throws IOException
    {
        ensureStoreVersion( fs, storeDir, files, ALL_STORES_VERSION );
    }

    public static void ensureStoreVersion( FileSystemAbstraction fs,
            File storeDir, Iterable<StoreFile20> files, String version ) throws IOException
    {
        for ( StoreFile20 file : files )
        {
            setStoreVersionTrailer( fs, new File( storeDir, file.storeFileName() ),
                    buildTypeDescriptorAndVersion( file.typeDescriptor(), version ) );
        }
        setStoreVersion( fs, new File( storeDir, DEFAULT_NAME ), versionStringToLong( version ) );
    }

    private static void setStoreVersionTrailer( FileSystemAbstraction fs,
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

    public static void deleteIdFile( FileSystemAbstraction fs, File directory, StoreFile20... stores )
    {
        for ( StoreFile20 store : stores )
        {
            fs.deleteFile( new File( directory, store.idFileName() ) );
        }
    }

    public static void deleteStoreFile( FileSystemAbstraction fs, File directory, StoreFile20... stores )
    {
        for ( StoreFile20 store : stores )
        {
            fs.deleteFile( new File( directory, store.storeFileName() ) );
        }
    }
}
