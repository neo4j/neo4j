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
package org.neo4j.coreedge.raft.log;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;

public class PhysicalRaftLogVerificationIT extends RaftLogVerificationIT
{
    @Override
    protected RaftLog createRaftLog() throws Throwable
    {
        FileSystemAbstraction fsa = fsRule.get();

        File directory = new File( "raft-log" );
        fsa.mkdir( directory );

        long rotateAtSizeBytes = 128;
        int entryCacheSize = 8;
        int fileHeaderCacheSize = 2;

        PhysicalRaftLog newRaftLog = new PhysicalRaftLog( fsa, directory, rotateAtSizeBytes, entryCacheSize, fileHeaderCacheSize,
                new PhysicalLogFile.Monitor.Adapter(), new DummyRaftableContentSerializer(), () -> mock( DatabaseHealth.class ),
                NullLogProvider.getInstance() );

        newRaftLog.init();
        newRaftLog.start();

        return newRaftLog;
    }

    @Override
    protected long operations()
    {
        return 500;
    }
}
