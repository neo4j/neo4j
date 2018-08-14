/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.neo4j.io.fs.FileUtils.getCanonicalFile;

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
        return file( DatabaseFileNames.METADATA_STORE );
    }

    public File labelScanStore()
    {
        return file( DatabaseFileNames.LABEL_SCAN_STORE );
    }

    public File countStoreA()
    {
        return file( DatabaseFileNames.COUNTS_STORE_A );
    }

    public File countStoreB()
    {
        return file( DatabaseFileNames.COUNTS_STORE_B );
    }

    public File propertyStringStore()
    {
        return file( DatabaseFileNames.PROPERTY_STRING_STORE );
    }

    public File relationshipStore()
    {
        return file( DatabaseFileNames.RELATIONSHIP_STORE );
    }

    public File propertyStore()
    {
        return file( DatabaseFileNames.PROPERTY_STORE );
    }

    public File nodeStore()
    {
        return file( DatabaseFileNames.NODE_STORE );
    }

    public File nodeLabelStore()
    {
        return file( DatabaseFileNames.NODE_LABELS_STORE );
    }

    public File propertyArrayStore()
    {
        return file( DatabaseFileNames.PROPERTY_ARRAY_STORE );
    }

    public File propertyKeyTokenStore()
    {
        return file( DatabaseFileNames.PROPERTY_KEY_TOKEN_STORE );
    }

    public File propertyKeyTokenNamesStore()
    {
        return file( DatabaseFileNames.PROPERTY_KEY_TOKEN_NAMES_STORE );
    }

    public File relationshipTypeTokenStore()
    {
        return file( DatabaseFileNames.RELATIONSHIP_TYPE_TOKEN_STORE );
    }

    public File relationshipTypeTokenNamesStore()
    {
        return file( DatabaseFileNames.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE );
    }

    public File labelTokenStore()
    {
        return file( DatabaseFileNames.LABEL_TOKEN_STORE );
    }

    public File schemaStore()
    {
        return file( DatabaseFileNames.SCHEMA_STORE );
    }

    public File relationshipGroupStore()
    {
        return file( DatabaseFileNames.RELATIONSHIP_GROUP_STORE );
    }

    public File labelTokenNamesStore()
    {
        return file( DatabaseFileNames.LABEL_TOKEN_NAMES_STORE );
    }

    public List<File> idFiles()
    {
        return Collections.emptyList();
    }

    public List<File> storeFiles()
    {
        return Collections.emptyList();
    }

    public File idFile( DatabaseStore store )
    {
        return idFile( store.getName() );
    }

    private File idFile( String name )
    {
        return file( idFileName( name ) );
    }

    private static String idFileName( String storeName )
    {
        return storeName + ".id";
    }

    //TODO: can we remove this now?
    public File file( String fileName )
    {
        return new File( databaseDirectory, fileName );
    }

    public File file( DatabaseStore databaseStore )
    {
        return new File( databaseDirectory, databaseStore.getName() );
    }

    //TODO: can we remove this now?
    public File directory( String directoryName )
    {
        return file( directoryName );
    }

    public File[] listDatabaseFiles( FilenameFilter filter )
    {
        File[] files = databaseDirectory.listFiles( filter );
        return files != null ? files : EMPTY_FILES_ARRAY;
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

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseDirectory, storeLayout );
    }

    public File idMetadataStore()
    {
        return idFile( DatabaseFileNames.METADATA_STORE );
    }

    public File idNodeStore()
    {
        return idFile( DatabaseFileNames.NODE_STORE );
    }

    public File idNodeLabelStore()
    {
        return idFile( DatabaseFileNames.NODE_LABELS_STORE );
    }

    public File idPropertyStore()
    {
        return idFile( DatabaseFileNames.PROPERTY_STORE );
    }

    public File idPropertyKeyTokenStore()
    {
        return idFile( DatabaseFileNames.PROPERTY_KEY_TOKEN_STORE );
    }

    public File idPropertyKeyTokenNamesStore()
    {
        return idFile( DatabaseFileNames.PROPERTY_KEY_TOKEN_NAMES_STORE );
    }

    public File idPropertyStringStore()
    {
        return idFile( DatabaseFileNames.PROPERTY_STRING_STORE );
    }

    public File idPropertyArrayStore()
    {
        return idFile( DatabaseFileNames.PROPERTY_ARRAY_STORE );
    }

    public File idRelationshipStore()
    {
        return idFile( DatabaseFileNames.RELATIONSHIP_STORE );
    }

    public File idRelationshipGroupStore()
    {
        return idFile( DatabaseFileNames.RELATIONSHIP_GROUP_STORE );
    }

    public File idRelationshipTypeTokenStore()
    {
        return idFile( DatabaseFileNames.RELATIONSHIP_TYPE_TOKEN_STORE );
    }

    public File idRelationshipTypeTokenNamesStore()
    {
        return idFile( DatabaseFileNames.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE );
    }

    public File idLabelTokenStore()
    {
        return idFile( DatabaseFileNames.LABEL_TOKEN_STORE );
    }

    public File idLabelTokenNamesStore()
    {
        return idFile( DatabaseFileNames.LABEL_TOKEN_NAMES_STORE );
    }

    public File idSchemaStore()
    {
        return idFile( DatabaseFileNames.SCHEMA_STORE );
    }
}
