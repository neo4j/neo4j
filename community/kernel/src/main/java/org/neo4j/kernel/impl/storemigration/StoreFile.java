/*
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.AbstractStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NeoStore.Position;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;

import static org.neo4j.helpers.Exceptions.withMessage;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.buildTypeDescriptorAndVersion;
import static org.neo4j.kernel.impl.store.NeoStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.NeoStore.setRecord;
import static org.neo4j.kernel.impl.store.NeoStore.versionStringToLong;

public enum StoreFile
{
    // all store files in Neo4j
    NODE_STORE(
            NodeStore.TYPE_DESCRIPTOR,
            StoreFactory.NODE_STORE_NAME,
            Legacy19Store.LEGACY_VERSION
    ),

    NODE_LABEL_STORE(
            DynamicArrayStore.TYPE_DESCRIPTOR,
            StoreFactory.NODE_LABELS_STORE_NAME,
            Legacy20Store.LEGACY_VERSION
    ),

    PROPERTY_STORE(
            PropertyStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_STORE_NAME,
            Legacy19Store.LEGACY_VERSION
    ),

    PROPERTY_ARRAY_STORE(
            DynamicArrayStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_ARRAYS_STORE_NAME,
            Legacy19Store.LEGACY_VERSION
    ),

    PROPERTY_STRING_STORE(
            DynamicStringStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_STRINGS_STORE_NAME,
            Legacy19Store.LEGACY_VERSION
    ),

    PROPERTY_KEY_TOKEN_STORE(
            PropertyKeyTokenStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME,
            Legacy19Store.LEGACY_VERSION
    ),

    PROPERTY_KEY_TOKEN_NAMES_STORE(
            DynamicStringStore.TYPE_DESCRIPTOR,
            StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME,
            Legacy19Store.LEGACY_VERSION
    ),

    RELATIONSHIP_STORE(
            RelationshipStore.TYPE_DESCRIPTOR,
            StoreFactory.RELATIONSHIP_STORE_NAME,
            Legacy19Store.LEGACY_VERSION
    ),

    RELATIONSHIP_GROUP_STORE(
            RelationshipGroupStore.TYPE_DESCRIPTOR,
            StoreFactory.RELATIONSHIP_GROUP_STORE_NAME,
            Legacy21Store.LEGACY_VERSION
    ),

    RELATIONSHIP_TYPE_TOKEN_STORE(
            RelationshipTypeTokenStore.TYPE_DESCRIPTOR,
            StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME,
            Legacy19Store.LEGACY_VERSION
    ),

    RELATIONSHIP_TYPE_TOKEN_NAMES_STORE(
            DynamicStringStore.TYPE_DESCRIPTOR,
            StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME,
            Legacy19Store.LEGACY_VERSION
    ),

    LABEL_TOKEN_STORE(
            LabelTokenStore.TYPE_DESCRIPTOR,
            StoreFactory.LABEL_TOKEN_STORE_NAME,
            Legacy20Store.LEGACY_VERSION
    ),

    LABEL_TOKEN_NAMES_STORE(
            DynamicStringStore.TYPE_DESCRIPTOR,
            StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME,
            Legacy20Store.LEGACY_VERSION
    ),

    SCHEMA_STORE(
            SchemaStore.TYPE_DESCRIPTOR,
            StoreFactory.SCHEMA_STORE_NAME,
            Legacy20Store.LEGACY_VERSION
    ),

    COUNTS_STORE_LEFT(
            CountsTracker.TYPE_DESCRIPTOR,
            StoreFactory.COUNTS_STORE + CountsTracker.LEFT,
            AbstractStore.ALL_STORES_VERSION
    )
            {
                @Override
                boolean isOptional()
                {
                    return true;
                }
            },
    COUNTS_STORE_RIGHT(
            CountsTracker.TYPE_DESCRIPTOR,
            StoreFactory.COUNTS_STORE + CountsTracker.RIGHT,
            AbstractStore.ALL_STORES_VERSION
    )
            {
                @Override
                boolean isOptional()
                {
                    return true;
                }
            },

    NEO_STORE(
            NeoStore.TYPE_DESCRIPTOR,
            "",
            Legacy19Store.LEGACY_VERSION
    );

    private final String typeDescriptor;
    private final String storeFileNamePart;
    private final String sinceVersion;

    private StoreFile( String typeDescriptor, String storeFileNamePart, String sinceVersion )
    {
        this.typeDescriptor = typeDescriptor;
        this.storeFileNamePart = storeFileNamePart;
        this.sinceVersion = sinceVersion;
    }

    public String forVersion( String version )
    {
        return typeDescriptor + " " + version;
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

    /**
     * @return the last part of the neostore filename, f.ex:
     *
     * <pre>
     * neostore.nodestore.db
     *         |           |
     *         <-this part-> (yes, including the leading dot)
     * </pre>
     */
    public String storeFileNamePart()
    {
        return storeFileNamePart;
    }

    public static Iterable<StoreFile> legacyStoreFilesForVersion( final String version )
    {
        Predicate<StoreFile> predicate = new Predicate<StoreFile>()
        {
            @Override
            public boolean accept( StoreFile item )
            {
                return version.compareTo( item.sinceVersion ) >= 0;
            }
        };

        Iterable<StoreFile> storeFiles = currentStoreFiles();
        Iterable<StoreFile> filter = Iterables.filter( predicate, storeFiles );
        return filter;
    }

    public static Iterable<StoreFile> currentStoreFiles()
    {
        return Iterables.iterable( values() );
    }

    /**
     * Performs a file operation on a database's store files from one directory
     * to another. Remember that in the case of {@link FileOperation#MOVE moving files}, the way that's done is to
     * just rename files (the standard way of moving with JDK6) from and to must be on the same disk partition.
     *
     * @param fromDirectory directory that hosts the database files.
     * @param toDirectory directory to receive the database files.
     * @throws IOException if any of the operations fail for any reason.
     */
    public static void fileOperation( FileOperation operation, FileSystemAbstraction fs, File fromDirectory,
            File toDirectory, Iterable<StoreFile> files,
            boolean allowSkipNonExistentFiles, boolean allowOverwriteTarget,
            StoreFileType... types ) throws IOException
    {
        // TODO: change the order of files to handle failure conditions properly
        for ( StoreFile storeFile : files )
        {
            for ( StoreFileType type : types )
            {
                String fileName = storeFile.fileName( type );
                operation.perform( fs, fileName,
                        fromDirectory, allowSkipNonExistentFiles, toDirectory, allowOverwriteTarget );
            }
        }
    }

    public static void ensureStoreVersion( FileSystemAbstraction fs,
                                           File storeDir, Iterable<StoreFile> files ) throws IOException
    {
        ensureStoreVersion( fs, storeDir, files, ALL_STORES_VERSION );
    }

    public static void ensureStoreVersion( FileSystemAbstraction fs,
                                           File storeDir, Iterable<StoreFile> files, String version ) throws IOException
    {
        for ( StoreFile file : files )
        {
            setStoreVersionTrailer( fs, new File( storeDir, file.storeFileName() ), file.isOptional(),
                    buildTypeDescriptorAndVersion( file.typeDescriptor(), version ) );
        }
        setRecord( fs, new File( storeDir, DEFAULT_NAME ), Position.STORE_VERSION, versionStringToLong( version ) );
    }

    boolean isOptional()
    {
        return false;
    }

    private static void setStoreVersionTrailer( FileSystemAbstraction fs, File targetStoreFileName, boolean optional,
                                                String versionTrailer ) throws IOException
    {
        byte[] trailer = UTF8.encode( versionTrailer );
        long fileSize = 0;
        if ( !fs.fileExists( targetStoreFileName ) )
        {
            if ( optional )
            {
                return;
            }
            else
            {
                throw new IllegalStateException( "Required file missing: " + targetStoreFileName );
            }
        }
        try ( StoreChannel fileChannel = fs.open( targetStoreFileName, "rw" ) )
        {
            fileSize = fileChannel.size();
            fileChannel.position( fileChannel.size() - trailer.length );
            fileChannel.write( ByteBuffer.wrap( trailer ) );
        }
        catch ( IllegalArgumentException e )
        {
            throw withMessage( e, e.getMessage() + " | " + "size:" + fileSize + ", trailer:" + trailer.length +
                    " for " + targetStoreFileName );
        }
    }
}
