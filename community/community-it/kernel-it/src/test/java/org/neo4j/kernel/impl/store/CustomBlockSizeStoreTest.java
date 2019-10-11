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

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.format.standard.DynamicRecordFormat;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
public class CustomBlockSizeStoreTest
{
    @Inject
    private GraphDatabaseAPI databaseAPI;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.string_block_size, 62 );
        builder.setConfig( GraphDatabaseSettings.array_block_size, 302 );
    }

    @Test
    public void testSetBlockSize()
    {
        var neoStores = databaseAPI.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        final PropertyStore propertyStore = neoStores.getPropertyStore();
        assertEquals( 62 + DynamicRecordFormat.RECORD_HEADER_SIZE, propertyStore.getStringStore().getRecordSize() );
        assertEquals( 302 + DynamicRecordFormat.RECORD_HEADER_SIZE, propertyStore.getArrayStore().getRecordSize() );
    }
}
