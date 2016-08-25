/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.convert;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.backup.BackupEmbeddedIT;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertNotEquals;

public class GenerateClusterSeedCommandTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldGenerateDifferentSeeds() throws Exception
    {
        // given
        File storeDir = testDirectory.graphDbDir();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        BackupEmbeddedIT.createSomeData( db );
        db.shutdown();

        FakeClock fakeClock = Clocks.fakeClock();
        fakeClock.forward( System.currentTimeMillis(), TimeUnit.MILLISECONDS );
        GenerateClusterSeedCommand command = new GenerateClusterSeedCommand( fakeClock );

        // when
        fakeClock.forward( 10, TimeUnit.MILLISECONDS );
        ClusterSeed seed1 = command.generate( storeDir );

        fakeClock.forward( 1, TimeUnit.MILLISECONDS );
        ClusterSeed seed2 = command.generate( storeDir );

        // then
        assertNotEquals( seed1, seed2 );
        assertNotEquals( seed1.getConversionId(), seed2.getConversionId() );
    }
}
