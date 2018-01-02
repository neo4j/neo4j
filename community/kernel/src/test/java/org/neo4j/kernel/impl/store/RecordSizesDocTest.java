/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.docs.DocsIncludeFile;

import static java.util.Arrays.asList;

import static org.neo4j.kernel.impl.store.StoreFactory.NODE_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_ARRAYS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STRINGS_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.RELATIONSHIP_STORE_NAME;
import static org.neo4j.kernel.impl.store.StoreFactory.Configuration.array_block_size;
import static org.neo4j.kernel.impl.store.StoreFactory.Configuration.string_block_size;

public class RecordSizesDocTest
{
    public final @Rule DocsIncludeFile writer = DocsIncludeFile.inSection( "ops" );

    @Test
    public void record_sizes_table() throws Exception
    {
        writer.println( "[options=\"header\",cols=\"<45,>20m,<35\", width=\"80%\"]" );
        writer.println( "|======================================" );
        writer.println( "| Store file  | Record size  | Contents" );
        for ( Store store : asList(
                store( NODE_STORE_NAME, NodeStore.RECORD_SIZE, "Nodes" ),
                store( RELATIONSHIP_STORE_NAME, RelationshipStore.RECORD_SIZE, "Relationships" ),
                store( PROPERTY_STORE_NAME, PropertyStore.RECORD_SIZE, "Properties for nodes and relationships" ),
                dynamicStore( PROPERTY_STRINGS_STORE_NAME, string_block_size, "Values of string properties" ),
                dynamicStore( PROPERTY_ARRAYS_STORE_NAME, array_block_size, "Values of array properties" )
        ) )
        {
            writer.printf( "| %s | %d B | %s%n", store.simpleFileName, store.recordSize, store.contentsDescription );
        }
        writer.println( "|======================================" );
        writer.println();
    }

    private static Store dynamicStore( String storeFileName, Setting<Integer> blockSizeSetting, String contentsDescription )
    {
        return store( storeFileName, defaultDynamicSize( blockSizeSetting ), contentsDescription );
    }

    private static Store store( String storeFileName, int recordSize, String contentsDescription )
    {
        return new Store( MetaDataStore.DEFAULT_NAME + storeFileName, recordSize, contentsDescription );
    }

    private static int defaultDynamicSize( Setting<Integer> setting )
    {
        return AbstractDynamicStore.BLOCK_HEADER_SIZE + Integer.parseInt( setting.getDefaultValue() );
    }

    private static class Store
    {
        final String simpleFileName;
        final int recordSize;
        final String contentsDescription;

        Store( String simpleFileName, int recordSize, String contentsDescription )
        {
            this.simpleFileName = simpleFileName;
            this.recordSize = recordSize;
            this.contentsDescription = contentsDescription;
        }
    }
}
