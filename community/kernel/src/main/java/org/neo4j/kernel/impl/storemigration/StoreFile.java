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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;

public enum StoreFile
{
    NEO_STORE( "NeoStore", "" ),
    NODE_STORE( "NodeStore", StoreFactory.NODE_STORE_NAME ),
    NODE_LABEL_STORE( "NodeLabelStore", StoreFactory.NODE_LABELS_STORE_NAME, false ),
    PROPERTY_STORE( "PropertyStore", StoreFactory.PROPERTY_STORE_NAME ),
    PROPERTY_ARRAY_STORE( "ArrayPropertyStore", StoreFactory.PROPERTY_ARRAYS_STORE_NAME ),
    PROPERTY_STRING_STORE( "StringPropertyStore", StoreFactory.PROPERTY_STRINGS_STORE_NAME ),
    PROPERTY_INDEX_STORE( "PropertyIndexStore", StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME ),
    PROPERTY_INDEX_KEYS_STORE( "StringPropertyStore", StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),
    RELATIONSHIP_STORE( "RelationshipStore", StoreFactory.RELATIONSHIP_STORE_NAME ),
    RELATIONSHIP_TYPE_STORE( "RelationshipTypeStore", StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME ),
    RELATIONSHIP_TYPE_NAMES_STORE( "StringPropertyStore", StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),
    LABEL_NAME_STORE( "LabelNameStore", StoreFactory.LABEL_TOKEN_STORE_NAME, false ),
    LABEL_NAME_NAMES_STORE( "StringPropertyStore", StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME, false ),
    SCHEMA_STORE( "SchemaStore", StoreFactory.SCHEMA_STORE_NAME, false );
    
    private final String typeDescriptor;
    private final String storeFileNamePart;
    private final boolean existsInBoth;

    private StoreFile( String typeDescriptor, String storeFileNamePart )
    {
        this( typeDescriptor, storeFileNamePart, true );
    }
    
    private StoreFile( String typeDescriptor, String storeFileNamePart, boolean existsInBoth )
    {
        this.typeDescriptor = typeDescriptor;
        this.storeFileNamePart = storeFileNamePart;
        this.existsInBoth = existsInBoth;
    }
    
    public String legacyVersion()
    {
        return typeDescriptor + " " + LegacyStore.LEGACY_VERSION;
    }

    /**
     * The first part of the version String.
     */
    public String typeDescriptor()
    {
        return typeDescriptor;
    }
    
    public String storeFileName()
    {
        return NeoStore.DEFAULT_NAME + storeFileNamePart;
    }
    
    public String idFileName()
    {
        return storeFileName() + ".id";
    }
    
    public static Iterable<StoreFile> legacyStoreFiles()
    {
        Predicate<StoreFile> predicate = new Predicate<StoreFile>()
        {
            @Override
            public boolean accept( StoreFile item )
            {
                return item.existsInBoth;
            }
        };
        Iterable<StoreFile> storeFiles = currentStoreFiles();
        return Iterables.filter( predicate, storeFiles );
    }
    
    public static Iterable<StoreFile> currentStoreFiles()
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
            Iterable<StoreFile> files ) throws IOException
    {
        // TODO: change the order that files are moved to handle failure conditions properly
        for ( StoreFile storeFile : files )
        {
            moveFile( fs, storeFile.storeFileName(), fromDirectory, toDirectory );
            moveFile( fs, storeFile.idFileName(), fromDirectory, toDirectory );
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
     * @throws IOException
     */
    static void moveFile( FileSystemAbstraction fs, String fileName, File fromDirectory,
            File toDirectory ) throws IOException
    {
        fs.moveToDirectory( new File( fromDirectory, fileName ), toDirectory );
    }
}
