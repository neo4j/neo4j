/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class SegmentedRaftLogRotationTest
{
    private static final int ROTATE_AT_SIZE_IN_BYTES = 100;

    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final LifeRule life = new LifeRule( true );
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
                                          .around( fileSystemRule ).around( life );

    @Test
    public void shouldRotateOnAppendWhenRotateSizeIsReached() throws Exception
    {
        // When
        SegmentedRaftLog log = life.add( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );
        log.append( new RaftLogEntry( 0, replicatedStringOfBytes( ROTATE_AT_SIZE_IN_BYTES ) ) );

        // Then
        File[] files = fileSystemRule.get().listFiles( testDirectory.directory() );
        assertEquals( 2, files.length );
    }

    @Test
    public void shouldBeAbleToRecoverToLatestStateAfterRotation() throws Throwable
    {
        // Given
        int term = 0;
        long indexToRestoreTo;
        try ( Lifespan lifespan = new Lifespan() )
        {
            SegmentedRaftLog log = lifespan.add( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );
            log.append( new RaftLogEntry( term, replicatedStringOfBytes( ROTATE_AT_SIZE_IN_BYTES - 40 ) ) );
            indexToRestoreTo = log.append( new RaftLogEntry( term, ReplicatedInteger.valueOf( 1 ) ) );
        }

        // When
        SegmentedRaftLog log = life.add( createRaftLog( ROTATE_AT_SIZE_IN_BYTES ) );

        // Then
        assertEquals( indexToRestoreTo, log.appendIndex() );
        assertEquals( term, log.readEntryTerm( indexToRestoreTo ) );
    }

    private ReplicatedString replicatedStringOfBytes( int size )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < size; i++ )
        {
            builder.append( "i" );
        }
        return new ReplicatedString( builder.toString() );
    }

    private SegmentedRaftLog createRaftLog( long rotateAtSize )
    {
        LogProvider logProvider = getInstance();
        CoreLogPruningStrategy pruningStrategy =
                new CoreLogPruningStrategyFactory( raft_log_pruning_strategy.getDefaultValue(), logProvider )
                        .newInstance();
        return new SegmentedRaftLog( fileSystemRule.get(), testDirectory.directory(), rotateAtSize,
                new DummyRaftableContentSerializer(), logProvider, 0, Clocks.fakeClock(), new OnDemandJobScheduler(),
                pruningStrategy );
    }
}
