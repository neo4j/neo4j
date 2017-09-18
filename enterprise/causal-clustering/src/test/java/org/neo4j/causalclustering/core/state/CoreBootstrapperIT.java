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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.Set;

import org.neo4j.causalclustering.backup.RestoreClusterUtils;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.machines.tx.LastCommittedIndexFinder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateType;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import static java.lang.Integer.parseInt;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_id_batch_size;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class CoreBootstrapperIT
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( pageCacheRule )
                                          .around( pageCacheRule ).around( testDirectory );

    @Test
    public void shouldSetAllCoreState() throws Exception
    {
        // given
        int nodeCount = 100;
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        File classicNeo4jStore = RestoreClusterUtils.createClassicNeo4jStore(
                testDirectory.directory(), fileSystem, nodeCount, Standard.LATEST_NAME );

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        CoreBootstrapper bootstrapper = new CoreBootstrapper( classicNeo4jStore, pageCache, fileSystem,
                Config.defaults(), NullLogProvider.getInstance() );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        int recordIdBatchSize = parseInt( record_id_batch_size.getDefaultValue() );
        assertThat( ((IdAllocationState) snapshot.get( CoreStateType.ID_ALLOCATION )).firstUnallocated( IdType.NODE ),
                allOf( greaterThanOrEqualTo( (long) nodeCount ), lessThanOrEqualTo( (long) nodeCount + recordIdBatchSize ) ) );

        /* Bootstrapped state is created in RAFT land at index -1 and term -1. */
        assertEquals( 0, snapshot.prevIndex() );
        assertEquals( 0, snapshot.prevTerm() );

        /* Lock is initially not taken. */
        assertEquals( new ReplicatedLockTokenState(), snapshot.get( CoreStateType.LOCK_TOKEN ) );

        /* Raft has the bootstrapped set of members initially. */
        assertEquals( membership, ((RaftCoreState) snapshot.get( CoreStateType.RAFT_CORE_STATE )).committed().members() );

        /* The session state is initially empty. */
        assertEquals( new GlobalSessionTrackerState(), snapshot.get( CoreStateType.SESSION_TRACKER ) );

        LastCommittedIndexFinder lastCommittedIndexFinder = new LastCommittedIndexFinder(
                new ReadOnlyTransactionIdStore( pageCache, classicNeo4jStore ),
                        new ReadOnlyTransactionStore( pageCache, fileSystem, classicNeo4jStore, new Monitors() ),
                        NullLogProvider.getInstance() );

        long lastCommittedIndex = lastCommittedIndexFinder.getLastCommittedIndex();
        assertEquals( -1, lastCommittedIndex );
    }

    private MemberId randomMember()
    {
        return new MemberId( randomUUID() );
    }
}
