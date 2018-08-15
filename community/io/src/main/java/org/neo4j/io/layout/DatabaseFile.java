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

public enum DatabaseFile
{
    NODE_STORE( DatabaseFileNames.NODE_STORE ),

    NODE_LABEL_STORE( DatabaseFileNames.NODE_LABELS_STORE ),

    PROPERTY_STORE( DatabaseFileNames.PROPERTY_STORE ),

    PROPERTY_ARRAY_STORE( DatabaseFileNames.PROPERTY_ARRAY_STORE ),

    PROPERTY_STRING_STORE( DatabaseFileNames.PROPERTY_STRING_STORE ),

    PROPERTY_KEY_TOKEN_STORE( DatabaseFileNames.PROPERTY_KEY_TOKEN_STORE ),

    PROPERTY_KEY_TOKEN_NAMES_STORE( DatabaseFileNames.PROPERTY_KEY_TOKEN_NAMES_STORE ),

    RELATIONSHIP_STORE( DatabaseFileNames.RELATIONSHIP_STORE ),

    RELATIONSHIP_GROUP_STORE( DatabaseFileNames.RELATIONSHIP_GROUP_STORE ),

    RELATIONSHIP_TYPE_TOKEN_STORE( DatabaseFileNames.RELATIONSHIP_TYPE_TOKEN_STORE ),

    RELATIONSHIP_TYPE_TOKEN_NAMES_STORE( DatabaseFileNames.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE ),

    LABEL_TOKEN_STORE( DatabaseFileNames.LABEL_TOKEN_STORE ),

    LABEL_TOKEN_NAMES_STORE( DatabaseFileNames.LABEL_TOKEN_NAMES_STORE ),

    SCHEMA_STORE( DatabaseFileNames.SCHEMA_STORE ),

    COUNTS_STORE_A( DatabaseFileNames.COUNTS_STORE_A, false ),
    COUNTS_STORE_B( DatabaseFileNames.COUNTS_STORE_B, false ),

    METADATA_STORE( DatabaseFileNames.METADATA_STORE ),

    LABEL_SCAN_STORE( DatabaseFileNames.LABEL_SCAN_STORE, false );

    private final String name;
    private final boolean hasIdFile;

    DatabaseFile( String name )
    {
        this( name, true );
    }

    DatabaseFile( String name, boolean hasIdFile )
    {
        this.name = name;
        this.hasIdFile = hasIdFile;
    }

    String getName()
    {
        return name;
    }

    public boolean hasIdFile()
    {
        return hasIdFile;
    }
}
