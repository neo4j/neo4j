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

import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class StoreUpgradeIntegrationTest
{
    @Test
    public void shouldUpgradeAutomaticallyOnDatabaseStartup() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() );
        prepareSampleLegacyDatabase( workingDirectory );

        assertTrue( allStoreFilesHaveVersion( workingDirectory, "v0.9.9" ) );

        HashMap params = new HashMap();
        params.put( Config.ALLOW_STORE_UPGRADE, "true" );

        GraphDatabaseService database = new EmbeddedGraphDatabase( workingDirectory.getPath(), params );
        database.shutdown();

        assertTrue( allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );
    }

}
