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
package org.neo4j.coreedge.raft.log.segmented;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.coreedge.raft.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogContractTest;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;

@RunWith(Parameterized.class)
public class SegmentedRaftLogContractTest extends RaftLogContractTest
{
    private SegmentedRaftLog raftLog;
    private LifeSupport life = new LifeSupport();
    private FileSystemAbstraction fileSystem;

    // parameter
    private int cacheSize;

    @Parameterized.Parameters(name = "cacheSize:{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][]{
            {0},
            {5},
            {1024},
        });
    }

    public SegmentedRaftLogContractTest( int cacheSize )
    {
        this.cacheSize = cacheSize;
    }

    @After
    public void tearDown() throws Throwable
    {
        life.stop();
        life.shutdown();
    }

    @Override
    public RaftLog createRaftLog()
    {
        if ( fileSystem == null )
        {
            fileSystem = new EphemeralFileSystemAbstraction();
        }
        File directory = new File( "raft-log" );
        fileSystem.mkdir( directory );

        SegmentedRaftLog newRaftLog = new SegmentedRaftLog( fileSystem, directory, 1024,
                new DummyRaftableContentSerializer(),
                NullLogProvider.getInstance(), cacheSize );
        life.add( newRaftLog );
        life.init();
        life.start();
        return newRaftLog;
    }
}
