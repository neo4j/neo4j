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
package org.neo4j.causalclustering.core.consensus.log.inmemory;

import java.io.File;

import org.neo4j.causalclustering.core.consensus.log.ConcurrentStressIT;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class InMemoryConcurrentStressIT extends ConcurrentStressIT<InMemoryConcurrentStressIT.LifecycledInMemoryRaftLog>
{
    @Override
    public LifecycledInMemoryRaftLog createRaftLog( FileSystemAbstraction fsa, File dir ) throws Throwable
    {
        return new LifecycledInMemoryRaftLog();
    }

    public static class LifecycledInMemoryRaftLog extends InMemoryRaftLog implements Lifecycle
    {

        @Override
        public void init() throws Throwable
        {

        }

        @Override
        public void start() throws Throwable
        {

        }

        @Override
        public void stop() throws Throwable
        {

        }

        @Override
        public void shutdown() throws Throwable
        {

        }
    }
}
