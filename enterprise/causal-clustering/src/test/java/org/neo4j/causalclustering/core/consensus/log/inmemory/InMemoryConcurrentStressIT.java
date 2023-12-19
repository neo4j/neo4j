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
package org.neo4j.causalclustering.core.consensus.log.inmemory;

import java.io.File;

import org.neo4j.causalclustering.core.consensus.log.ConcurrentStressIT;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class InMemoryConcurrentStressIT extends ConcurrentStressIT<InMemoryConcurrentStressIT.LifecycledInMemoryRaftLog>
{
    @Override
    public LifecycledInMemoryRaftLog createRaftLog( FileSystemAbstraction fsa, File dir )
    {
        return new LifecycledInMemoryRaftLog();
    }

    public static class LifecycledInMemoryRaftLog extends InMemoryRaftLog implements Lifecycle
    {

        @Override
        public void init()
        {

        }

        @Override
        public void start()
        {

        }

        @Override
        public void stop()
        {

        }

        @Override
        public void shutdown()
        {

        }
    }
}
