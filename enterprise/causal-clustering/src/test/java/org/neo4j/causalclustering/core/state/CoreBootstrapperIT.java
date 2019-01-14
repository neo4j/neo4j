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
package org.neo4j.causalclustering.core.state;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.machines.tx.LastCommittedIndexFinder;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreStateType;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.helpers.ClassicNeo4jStore;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Integer.parseInt;
import static java.util.UUID.randomUUID;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_id_batch_size;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class CoreBootstrapperIT
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( pageCacheRule ).around( pageCacheRule ).around( testDirectory );

    @Test
    public void shouldSetAllCoreState() throws Exception
    {
        // given
        int nodeCount = 100;
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        File classicNeo4jStore = ClassicNeo4jStore.builder( testDirectory.directory(), fileSystem ).amountOfNodes( nodeCount ).build().getStoreDir();

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        CoreBootstrapper bootstrapper =
                new CoreBootstrapper( classicNeo4jStore, pageCache, fileSystem, Config.defaults(), NullLogProvider.getInstance(), new Monitors() );
        bootstrapAndVerify( nodeCount, fileSystem, classicNeo4jStore, pageCache, Config.defaults(), bootstrapper );
    }

    @Test
    public void setAllCoreStateOnDatabaseWithCustomLogFilesLocation() throws Exception
    {
        // given
        int nodeCount = 100;
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        String customTransactionLogsLocation = "transaction-logs";
        File classicNeo4jStore = ClassicNeo4jStore
                .builder( testDirectory.directory(), fileSystem )
                .amountOfNodes( nodeCount )
                .logicalLogsLocation( customTransactionLogsLocation )
                .build()
                .getStoreDir();

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config config = Config.defaults( GraphDatabaseSettings.logical_logs_location, customTransactionLogsLocation );
        CoreBootstrapper bootstrapper = new CoreBootstrapper( classicNeo4jStore, pageCache, fileSystem, config, NullLogProvider.getInstance(), new Monitors() );

        bootstrapAndVerify( nodeCount, fileSystem, classicNeo4jStore, pageCache, config, bootstrapper );
    }

    @Test
    public void shouldFailToBootstrapIfClusterIsInNeedOfRecovery() throws IOException
    {
        // given
        int nodeCount = 100;
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        File storeInNeedOfRecovery =
                ClassicNeo4jStore.builder( testDirectory.directory(), fileSystem ).amountOfNodes( nodeCount ).needToRecover().build().getStoreDir();
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider(  );

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        CoreBootstrapper bootstrapper =
                new CoreBootstrapper( storeInNeedOfRecovery, pageCache, fileSystem, Config.defaults(), assertableLogProvider, new Monitors() );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        try
        {
            bootstrapper.bootstrap( membership );
            fail();
        }
        catch ( IllegalStateException e )
        {
            String errorMessage = "Cannot bootstrap. Recovery is required. Please ensure that the store being seeded comes from a cleanly shutdown " +
                    "instance of Neo4j or a Neo4j backup";
            assertEquals( e.getMessage(), errorMessage );
            assertableLogProvider.assertExactly( AssertableLogProvider.inLog( CoreBootstrapper.class ).error( errorMessage) );
        }
    }

    @Test
    public void shouldFailToBootstrapIfClusterIsInNeedOfRecoveryWithCustomLogicalLogsLocation() throws IOException
    {
        // given
        int nodeCount = 100;
        FileSystemAbstraction fileSystem = fileSystemRule.get();
        String customTransactionLogsLocation = "transaction-logs";
        File storeInNeedOfRecovery = ClassicNeo4jStore
                .builder( testDirectory.directory(), fileSystem )
                .amountOfNodes( nodeCount )
                .logicalLogsLocation( customTransactionLogsLocation )
                .needToRecover()
                .build()
                .getStoreDir();
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider(  );

        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config config = Config.defaults( GraphDatabaseSettings.logical_logs_location, customTransactionLogsLocation );
        CoreBootstrapper bootstrapper =
                new CoreBootstrapper( storeInNeedOfRecovery, pageCache, fileSystem, config, assertableLogProvider, new Monitors() );

        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        try
        {
            bootstrapper.bootstrap( membership );
            fail();
        }
        catch ( IllegalStateException e )
        {
            String errorMessage = "Cannot bootstrap. Recovery is required. Please ensure that the store being seeded comes from a cleanly shutdown " +
                    "instance of Neo4j or a Neo4j backup";
            assertEquals( e.getMessage(), errorMessage );
            assertableLogProvider.assertExactly( AssertableLogProvider.inLog( CoreBootstrapper.class ).error( errorMessage) );
        }
    }

    private void bootstrapAndVerify( long nodeCount, FileSystemAbstraction fileSystem, File classicNeo4jStore,
            PageCache pageCache, Config config, CoreBootstrapper bootstrapper ) throws IOException
    {
        // when
        Set<MemberId> membership = asSet( randomMember(), randomMember(), randomMember() );
        CoreSnapshot snapshot = bootstrapper.bootstrap( membership );

        // then
        int recordIdBatchSize = parseInt( record_id_batch_size.getDefaultValue() );
        assertThat( ((IdAllocationState) snapshot.get( CoreStateType.ID_ALLOCATION )).firstUnallocated( IdType.NODE ),
                allOf( greaterThanOrEqualTo( nodeCount ), lessThanOrEqualTo( nodeCount + recordIdBatchSize ) ) );

        /* Bootstrapped state is created in RAFT land at index -1 and term -1. */
        assertEquals( 0, snapshot.prevIndex() );
        assertEquals( 0, snapshot.prevTerm() );

        /* Lock is initially not taken. */
        assertEquals( new ReplicatedLockTokenState(), snapshot.get( CoreStateType.LOCK_TOKEN ) );

        /* Raft has the bootstrapped set of members initially. */
        assertEquals( membership, ((RaftCoreState) snapshot.get( CoreStateType.RAFT_CORE_STATE )).committed().members() );

        /* The session state is initially empty. */
        assertEquals( new GlobalSessionTrackerState(), snapshot.get( CoreStateType.SESSION_TRACKER ) );

        ReadOnlyTransactionStore transactionStore = new ReadOnlyTransactionStore( pageCache, fileSystem,
                classicNeo4jStore, config, new Monitors() );
        LastCommittedIndexFinder lastCommittedIndexFinder = new LastCommittedIndexFinder(
                new ReadOnlyTransactionIdStore( pageCache, classicNeo4jStore ),
                transactionStore, NullLogProvider.getInstance() );

        long lastCommittedIndex = lastCommittedIndexFinder.getLastCommittedIndex();
        assertEquals( -1, lastCommittedIndex );
    }

    private MemberId randomMember()
    {
        return new MemberId( randomUUID() );
    }
}
