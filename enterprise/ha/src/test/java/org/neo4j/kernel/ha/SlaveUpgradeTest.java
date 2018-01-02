/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByDatabaseModeException;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SlaveUpgradeTest
{
    @Test
    public void haShouldFailToStartWithOldStore() throws Exception
    {
        try
        {
            File dir = testDirectory.directory( "haShouldFailToStartWithOldStore" );
            MigrationTestUtils.find20FormatStoreDirectory( dir );

            new TestHighlyAvailableGraphDatabaseFactory()
                    .newHighlyAvailableDatabaseBuilder( dir.getAbsolutePath() )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( ClusterSettings.initial_hosts, "localhost:9999" ) // Mandatory setting, irrelevant for this test though, just needs to be here
                    .newGraphDatabase();

            fail( "Should exit abnormally" );
        }
        catch ( Exception e )
        {
            Throwable rootCause = Exceptions.rootCause( e );
            assertThat( rootCause, instanceOf( UpgradeNotAllowedByDatabaseModeException.class ) );
        }
    }

    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
}
