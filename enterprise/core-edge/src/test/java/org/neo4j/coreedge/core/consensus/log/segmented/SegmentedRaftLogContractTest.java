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
package org.neo4j.coreedge.core.consensus.log.segmented;

import org.junit.After;

import java.io.File;

import org.neo4j.coreedge.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.core.consensus.log.RaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLogContractTest;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.FakeClock;

import static org.neo4j.coreedge.core.consensus.log.segmented.SegmentedRaftLog.SEGMENTED_LOG_DIRECTORY_NAME;

public class SegmentedRaftLogContractTest extends RaftLogContractTest
{
    private LifeSupport life = new LifeSupport();
    private FileSystemAbstraction fileSystem;

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

        File directory = new File( SEGMENTED_LOG_DIRECTORY_NAME );
        fileSystem.mkdir( directory );

        SegmentedRaftLog newRaftLog = new SegmentedRaftLog( fileSystem, directory, 1024,
                new DummyRaftableContentSerializer(),
                NullLogProvider.getInstance(), "1 entries", 8, new FakeClock(), new OnDemandJobScheduler() );

        life.add( newRaftLog );
        life.init();
        life.start();
        return newRaftLog;
    }
}
