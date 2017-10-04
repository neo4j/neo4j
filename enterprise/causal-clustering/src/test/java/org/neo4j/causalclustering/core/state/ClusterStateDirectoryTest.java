/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class ClusterStateDirectoryTest
{
    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();
    private FileSystemAbstraction fs = fsRule.get();

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fs );

    private File dataDir;
    private File stateDir;

    @Before
    public void setup()
    {
        dataDir = testDirectory.directory( "data" );
        stateDir = new File( dataDir, ClusterStateDirectory.CLUSTER_STATE_DIRECTORY_NAME );
    }

    @Test
    public void shouldMigrateClusterStateFromStoreDir() throws Exception
    {
        // given
        File storeDir = new File( new File( dataDir, "databases" ), "graph.db" );

        String fileName = "file";

        File oldStateDir = new File( storeDir, ClusterStateDirectory.CLUSTER_STATE_DIRECTORY_NAME );
        File oldClusterStateFile = new File( oldStateDir, fileName );

        fs.mkdirs( oldStateDir );
        fs.create( oldClusterStateFile ).close();

        // when
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, storeDir, false );
        clusterStateDirectory.initialize( fs );

        // then
        assertEquals( clusterStateDirectory.get(), stateDir );
        assertTrue( fs.fileExists( new File( clusterStateDirectory.get(), fileName ) ) );
    }

    @Test
    public void shouldHandleCaseOfStoreDirBeingDataDir() throws Exception
    {
        // given
        File storeDir = dataDir;

        String fileName = "file";

        File oldStateDir = new File( storeDir, ClusterStateDirectory.CLUSTER_STATE_DIRECTORY_NAME );
        File oldClusterStateFile = new File( oldStateDir, fileName );

        fs.mkdirs( oldStateDir );
        fs.create( oldClusterStateFile ).close();

        // when
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, storeDir, false );
        clusterStateDirectory.initialize( fs );

        // then
        assertEquals( clusterStateDirectory.get(), stateDir );
        assertTrue( fs.fileExists( new File( clusterStateDirectory.get(), fileName ) ) );
    }
}
