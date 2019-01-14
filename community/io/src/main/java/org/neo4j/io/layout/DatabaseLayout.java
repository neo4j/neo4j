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
package org.neo4j.io.layout;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.stream.Streams;

import static org.neo4j.io.fs.FileUtils.getCanonicalFile;

/**
 * File layout representation of the particular database. Facade for any kind of file lookup for a particular database storage implementation.
 * Any file retrieved from a layout can be considered a canonical file.
 * <br/>
 * No assumption should be made about where and how files of a particular database are positioned and all those details should be encapsulated inside.
 *
 * @see StoreLayout
 * @see DatabaseFile
 */
public class DatabaseLayout
{
    private static final File[] EMPTY_FILES_ARRAY = new File[0];
    private final File databaseDirectory;
    private final StoreLayout storeLayout;
    private final String databaseName;

    public static DatabaseLayout of( StoreLayout storeLayout, String databaseName )
    {
        return new DatabaseLayout( storeLayout, databaseName );
    }

    public static DatabaseLayout of( File databaseDirectory )
    {
        File canonicalFile = getCanonicalFile( databaseDirectory );
        return of( canonicalFile.getParentFile(), canonicalFile.getName() );
    }

    public static DatabaseLayout of( File rootDirectory, String databaseName )
    {
        return new DatabaseLayout( StoreLayout.of( rootDirectory ), databaseName );
    }

    private DatabaseLayout( StoreLayout storeLayout, String databaseName )
    {
        this.storeLayout = storeLayout;
        this.databaseDirectory = new File( storeLayout.storeDirectory(), databaseName );
        this.databaseName = databaseName;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public StoreLayout getStoreLayout()
    {
        return storeLayout;
    }

    public File databaseDirectory()
    {
        return databaseDirectory;
    }

    public File metadataStore()
    {
        return file( DatabaseFile.METADATA_STORE.getName() );
    }

    public File labelScanStore()
    {
        return file( DatabaseFile.LABEL_SCAN_STORE.getName() );
    }

    public File countStoreA()
    {
        return file( DatabaseFile.COUNTS_STORE_A.getName() );
    }

    public File countStoreB()
    {
        return file( DatabaseFile.COUNTS_STORE_B.getName() );
    }

    public File propertyStringStore()
    {
        return file( DatabaseFile.PROPERTY_STRING_STORE.getName() );
    }

    public File relationshipStore()
    {
        return file( DatabaseFile.RELATIONSHIP_STORE.getName() );
    }

    public File propertyStore()
    {
        return file( DatabaseFile.PROPERTY_STORE.getName() );
    }

    public File nodeStore()
    {
        return file( DatabaseFile.NODE_STORE.getName() );
    }

    public File nodeLabelStore()
    {
        return file( DatabaseFile.NODE_LABEL_STORE.getName() );
    }

    public File propertyArrayStore()
    {
        return file( DatabaseFile.PROPERTY_ARRAY_STORE.getName() );
    }

    public File propertyKeyTokenStore()
    {
        return file( DatabaseFile.PROPERTY_KEY_TOKEN_STORE.getName() );
    }

    public File propertyKeyTokenNamesStore()
    {
        return file( DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE.getName() );
    }

    public File relationshipTypeTokenStore()
    {
        return file( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE.getName() );
    }

    public File relationshipTypeTokenNamesStore()
    {
        return file( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE.getName() );
    }

    public File labelTokenStore()
    {
        return file( DatabaseFile.LABEL_TOKEN_STORE.getName() );
    }

    public File schemaStore()
    {
        return file( DatabaseFile.SCHEMA_STORE.getName() );
    }

    public File relationshipGroupStore()
    {
        return file( DatabaseFile.RELATIONSHIP_GROUP_STORE.getName() );
    }

    public File labelTokenNamesStore()
    {
        return file( DatabaseFile.LABEL_TOKEN_NAMES_STORE.getName() );
    }

    public Set<File> idFiles()
    {
        return Arrays.stream( DatabaseFile.values() )
                     .filter( DatabaseFile::hasIdFile )
                    .flatMap( value -> Streams.ofOptional( idFile( value ) ) )
                     .collect( Collectors.toSet() );
    }

    public Set<File> storeFiles()
    {
        return Arrays.stream( DatabaseFile.values() )
                .flatMap( this::file )
                .collect( Collectors.toSet() );
    }

    public Optional<File> idFile( DatabaseFile file )
    {
        return file.hasIdFile() ? Optional.of( idFile( file.getName() ) ) : Optional.empty();
    }

    public File file( String fileName )
    {
        return new File( databaseDirectory, fileName );
    }

    public Stream<File> file( DatabaseFile databaseFile )
    {
        Iterable<String> names = databaseFile.getNames();
        return Iterables.stream( names ).map( this::file );
    }

    public File[] listDatabaseFiles( FilenameFilter filter )
    {
        File[] files = databaseDirectory.listFiles( filter );
        return files != null ? files : EMPTY_FILES_ARRAY;
    }

    public File idMetadataStore()
    {
        return idFile( DatabaseFile.METADATA_STORE.getName() );
    }

    public File idNodeStore()
    {
        return idFile( DatabaseFile.NODE_STORE.getName() );
    }

    public File idNodeLabelStore()
    {
        return idFile( DatabaseFile.NODE_LABEL_STORE.getName() );
    }

    public File idPropertyStore()
    {
        return idFile( DatabaseFile.PROPERTY_STORE.getName() );
    }

    public File idPropertyKeyTokenStore()
    {
        return idFile( DatabaseFile.PROPERTY_KEY_TOKEN_STORE.getName() );
    }

    public File idPropertyKeyTokenNamesStore()
    {
        return idFile( DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE.getName() );
    }

    public File idPropertyStringStore()
    {
        return idFile( DatabaseFile.PROPERTY_STRING_STORE.getName() );
    }

    public File idPropertyArrayStore()
    {
        return idFile( DatabaseFile.PROPERTY_ARRAY_STORE.getName() );
    }

    public File idRelationshipStore()
    {
        return idFile( DatabaseFile.RELATIONSHIP_STORE.getName() );
    }

    public File idRelationshipGroupStore()
    {
        return idFile( DatabaseFile.RELATIONSHIP_GROUP_STORE.getName() );
    }

    public File idRelationshipTypeTokenStore()
    {
        return idFile( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE.getName() );
    }

    public File idRelationshipTypeTokenNamesStore()
    {
        return idFile( DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE.getName() );
    }

    public File idLabelTokenStore()
    {
        return idFile( DatabaseFile.LABEL_TOKEN_STORE.getName() );
    }

    public File idLabelTokenNamesStore()
    {
        return idFile( DatabaseFile.LABEL_TOKEN_NAMES_STORE.getName() );
    }

    public File idSchemaStore()
    {
        return idFile( DatabaseFile.SCHEMA_STORE.getName() );
    }

    private File idFile( String name )
    {
        return file( idFileName( name ) );
    }

    private static String idFileName( String storeName )
    {
        return storeName + ".id";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseDirectory, storeLayout );
    }

    @Override
    public String toString()
    {
        return String.valueOf( databaseDirectory );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        DatabaseLayout that = (DatabaseLayout) o;
        return Objects.equals( databaseDirectory, that.databaseDirectory ) && Objects.equals( storeLayout, that.storeLayout );
    }
}
