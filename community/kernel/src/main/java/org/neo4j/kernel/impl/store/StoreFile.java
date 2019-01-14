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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.storemigration.ExistingTargetStrategy;
import org.neo4j.kernel.impl.storemigration.FileOperation;
import org.neo4j.kernel.impl.storemigration.StoreFileType;

public enum StoreFile
{
    // all store files in Neo4j
    NODE_STORE( StoreFactory.NODE_STORE_NAME ),

    NODE_LABEL_STORE( StoreFactory.NODE_LABELS_STORE_NAME ),

    PROPERTY_STORE( StoreFactory.PROPERTY_STORE_NAME ),

    PROPERTY_ARRAY_STORE( StoreFactory.PROPERTY_ARRAYS_STORE_NAME ),

    PROPERTY_STRING_STORE( StoreFactory.PROPERTY_STRINGS_STORE_NAME ),

    PROPERTY_KEY_TOKEN_STORE( StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME ),

    PROPERTY_KEY_TOKEN_NAMES_STORE( StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),

    RELATIONSHIP_STORE( StoreFactory.RELATIONSHIP_STORE_NAME ),

    RELATIONSHIP_GROUP_STORE( StoreFactory.RELATIONSHIP_GROUP_STORE_NAME ),

    RELATIONSHIP_TYPE_TOKEN_STORE( StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME ),

    RELATIONSHIP_TYPE_TOKEN_NAMES_STORE( StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),

    LABEL_TOKEN_STORE( StoreFactory.LABEL_TOKEN_STORE_NAME ),

    LABEL_TOKEN_NAMES_STORE( StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME ),

    SCHEMA_STORE( StoreFactory.SCHEMA_STORE_NAME ),

    COUNTS_STORE_LEFT( StoreFactory.COUNTS_STORE + CountsTracker.LEFT, false )
            {
                @Override
                public boolean isOptional()
                {
                    return true;
                }
            },
    COUNTS_STORE_RIGHT( StoreFactory.COUNTS_STORE + CountsTracker.RIGHT, false )
            {
                @Override
                public boolean isOptional()
                {
                    return true;
                }
            },

    NEO_STORE( "" );

    private final String storeFileNamePart;
    private final boolean recordStore;

    StoreFile( String storeFileNamePart )
    {
        this( storeFileNamePart, true );
    }

    StoreFile( String storeFileNamePart, boolean recordStore )
    {
        this.storeFileNamePart = storeFileNamePart;
        this.recordStore = recordStore;
    }

    public String fileName( StoreFileType type )
    {
        return type.augment( MetaDataStore.DEFAULT_NAME + storeFileNamePart );
    }

    public String storeFileName()
    {
        return fileName( StoreFileType.STORE );
    }

    public String fileNamePart()
    {
        return storeFileNamePart;
    }

    public boolean isRecordStore()
    {
        return recordStore;
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
            boolean allowSkipNonExistentFiles, ExistingTargetStrategy existingTargetStrategy,
            StoreFileType... types ) throws IOException
    {
        // TODO: change the order of files to handle failure conditions properly
        for ( StoreFile storeFile : files )
        {
            for ( StoreFileType type : types )
            {
                String fileName = storeFile.fileName( type );
                operation.perform( fs, fileName,
                        fromDirectory, allowSkipNonExistentFiles, toDirectory, existingTargetStrategy );
            }
        }
    }

    public boolean isOptional()
    {
        return false;
    }
}
