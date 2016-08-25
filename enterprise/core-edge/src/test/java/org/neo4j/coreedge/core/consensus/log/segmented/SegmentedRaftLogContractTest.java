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

import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.coreedge.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.coreedge.core.consensus.log.RaftLog;
import org.neo4j.coreedge.core.consensus.log.RaftLogContractTest;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.time.Clocks;

import static org.neo4j.coreedge.core.consensus.log.RaftLog.PHYSICAL_LOG_DIRECTORY_NAME;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedRaftLogContractTest extends RaftLogContractTest
{
    private final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final LifeRule life = new LifeRule( true );

    @Rule
    public RuleChain chain = RuleChain.outerRule( fsRule ).around( life );

    @Override
    public RaftLog createRaftLog()
    {
        File directory = new File( PHYSICAL_LOG_DIRECTORY_NAME );
        FileSystemAbstraction fileSystem = fsRule.get();
        fileSystem.mkdir( directory );

        return life.add( new SegmentedRaftLog( fileSystem, directory, 1024, new DummyRaftableContentSerializer(),
                getInstance(), "1 entries", 8, Clocks.fakeClock(), new OnDemandJobScheduler() ) );
    }
}
