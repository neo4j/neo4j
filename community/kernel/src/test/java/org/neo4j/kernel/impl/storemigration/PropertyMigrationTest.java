/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.util.FileUtils;

public class PropertyMigrationTest
{
    @Test
    public void shouldRewrite() throws IOException
    {
        URL legacyNodeStoreFile = getClass().getResource( "oldformatstore/neostore.nodestore.db" );
        URL legacyPropertyStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db" );
        URL legacyStringStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db.strings" );
        URL legacyArrayStoreFile = getClass().getResource( "oldformatstore/neostore.propertystore.db.arrays" );

        LegacyPropertyStoreReader propertyStoreReader = new LegacyPropertyStoreReader( legacyPropertyStoreFile.getFile() );
        LegacyDynamicRecordFetcher legacyDynamicRecordFetcher = new LegacyDynamicRecordFetcher( legacyStringStoreFile.getFile(), legacyArrayStoreFile.getFile() );
        LegacyNodeStoreReader legacyNodeStoreReader = new LegacyNodeStoreReader( legacyNodeStoreFile.getFile() );

        HashMap config = MigrationTestUtils.defaultConfig();
        File outputDir = new File( "target/outputDatabase" );
        FileUtils.deleteRecursively( outputDir );
        assertTrue( outputDir.mkdirs() );

        File propertyStoreFile = new File( outputDir, "neostore.propertystore.db" );
        PropertyStore.createStore( propertyStoreFile.getPath(), config );
        PropertyStore propertyStore = new PropertyStore( propertyStoreFile.getPath(), config );

        new PropertyMigration( legacyNodeStoreReader, propertyStoreReader, legacyDynamicRecordFetcher ).migrateNodeProperties( new PropertyWriter( propertyStore  ));

        assertEquals( 1000, propertyStore.getHighId() );

        propertyStore.close();
    }

}
