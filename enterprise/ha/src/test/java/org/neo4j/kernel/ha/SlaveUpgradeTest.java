/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class SlaveUpgradeTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void haShouldFailToStartWithOldStore()
    {
        try
        {
            File dir = testDirectory.directory( "haShouldFailToStartWithOldStore" );
            MigrationTestUtils.find23FormatStoreDirectory( dir );

            new TestHighlyAvailableGraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder( dir )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( ClusterSettings.initial_hosts, "localhost:9999" )
                    .newGraphDatabase();

            fail( "Should exit abnormally" );
        }
        catch ( Exception e )
        {
            Throwable rootCause = Exceptions.rootCause( e );
            assertThat( rootCause, instanceOf( UpgradeNotAllowedByConfigurationException.class ) );
        }
    }
}
