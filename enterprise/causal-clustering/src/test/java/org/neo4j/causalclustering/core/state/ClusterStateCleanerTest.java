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
package org.neo4j.causalclustering.core.state;

import org.junit.Test;

import java.io.File;

import org.neo4j.causalclustering.catchup.storecopy.LocalDatabase;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterStateCleanerTest
{

    @Test
    public void shouldDeleteClusterStateInTheEventOfAnEmptyDatabase() throws Throwable
    {
        // given
        LocalDatabase db = mock( LocalDatabase.class );
        when( db.isEmpty() ).thenReturn( true ).thenReturn( false );
        ClusterStateDirectory clusterStateDirectory = mock( ClusterStateDirectory.class );
        when( clusterStateDirectory.isEmpty() ).thenReturn( false );
        File stateDir = mock( File.class );
        when( clusterStateDirectory.get() ).thenReturn( stateDir );
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );

        ClusterStateCleaner clusterStateCleaner1 = new ClusterStateCleaner( db, clusterStateDirectory, fs, NullLogProvider.getInstance() );
        ClusterStateCleaner clusterStateCleaner2 = new ClusterStateCleaner( db, clusterStateDirectory, fs, NullLogProvider.getInstance() );

        // when
        clusterStateCleaner1.init();
        clusterStateCleaner2.init();

        // then
        verify( clusterStateDirectory, times( 1 ) ).clear( fs );
    }
}
