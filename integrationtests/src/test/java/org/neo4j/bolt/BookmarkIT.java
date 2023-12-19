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
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class BookmarkIT
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( getClass() );

    private Driver driver;
    private GraphDatabaseAPI db;

    @After
    public void tearDown() throws Exception
    {
        IOUtils.closeAllSilently( driver );
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void shouldReturnUpToDateBookmarkWhenSomeTransactionIsCommitting() throws Exception
    {
        CommitBlocker commitBlocker = new CommitBlocker();
        db = createDb( commitBlocker );
        driver = GraphDatabase.driver( boltAddress( db ) );

        String firstBookmark = createNode( driver );

        // make next transaction append to the log and then pause before applying to the store
        // this makes it allocate a transaction ID but wait before acknowledging the commit operation
        commitBlocker.blockNextTransaction();
        CompletableFuture<String> secondBookmarkFuture = CompletableFuture.supplyAsync( () -> createNode( driver ) );
        assertEventually( "Transaction did not block as expected", commitBlocker::hasBlockedTransaction, is( true ), 1, MINUTES );

        Set<String> otherBookmarks = Stream.generate( () -> createNode( driver ) )
                .limit( 10 )
                .collect( toSet() );

        commitBlocker.unblock();
        String lastBookmark = secondBookmarkFuture.get();

        // first and last bookmarks should not be null and should be different
        assertNotNull( firstBookmark );
        assertNotNull( lastBookmark );
        assertNotEquals( firstBookmark, lastBookmark );

        // all bookmarks received while a transaction was blocked committing should be unique
        assertThat( otherBookmarks, hasSize( 10 ) );
    }

    private GraphDatabaseAPI createDb( CommitBlocker commitBlocker )
    {
        return createDb( platformModule -> new CustomCommunityEditionModule( platformModule, commitBlocker ) );
    }

    private GraphDatabaseAPI createDb( Function<PlatformModule,EditionModule> editionModuleFactory )
    {
        GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
        GraphDatabaseFacadeFactory facadeFactory = new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, editionModuleFactory );
        return facadeFactory.newFacade( directory.graphDbDir(), configWithBoltEnabled(), state.databaseDependencies() );
    }

    private static String createNode( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE ()" );
                tx.success();
            }
            return session.lastBookmark();
        }
    }

    private static Config configWithBoltEnabled()
    {
        Config config = Config.defaults();

        config.augment( "dbms.connector.bolt.enabled", TRUE );
        config.augment( "dbms.connector.bolt.listen_address", "localhost:0" );

        return config;
    }

    private static String boltAddress( GraphDatabaseAPI db )
    {
        ConnectorPortRegister portRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        return "bolt://" + portRegister.getLocalAddress( "bolt" );
    }

    private static class CustomCommunityEditionModule extends CommunityEditionModule
    {
        CustomCommunityEditionModule( PlatformModule platformModule, CommitBlocker commitBlocker )
        {
            super( platformModule );
            commitProcessFactory = new CustomCommitProcessFactory( commitBlocker );
        }
    }

    private static class CustomCommitProcessFactory implements CommitProcessFactory
    {
        final CommitBlocker commitBlocker;

        private CustomCommitProcessFactory( CommitBlocker commitBlocker )
        {
            this.commitBlocker = commitBlocker;
        }

        @Override
        public TransactionCommitProcess create( TransactionAppender appender, StorageEngine storageEngine, Config config )
        {
            return new CustomCommitProcess( appender, storageEngine, commitBlocker );
        }
    }

    private static class CustomCommitProcess extends TransactionRepresentationCommitProcess
    {
        final CommitBlocker commitBlocker;

        CustomCommitProcess( TransactionAppender appender, StorageEngine storageEngine, CommitBlocker commitBlocker )
        {
            super( appender, storageEngine );
            this.commitBlocker = commitBlocker;
        }

        @Override
        protected void applyToStore( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode ) throws TransactionFailureException
        {
            commitBlocker.blockWhileWritingToStoreIfNeeded();
            super.applyToStore( batch, commitEvent, mode );
        }
    }

    private static class CommitBlocker
    {
        final ReentrantLock lock = new ReentrantLock();
        volatile boolean shouldBlock;

        void blockNextTransaction()
        {
            shouldBlock = true;
            lock.lock();
        }

        void blockWhileWritingToStoreIfNeeded()
        {
            if ( shouldBlock )
            {
                shouldBlock = false;
                lock.lock();
            }
        }

        void unblock()
        {
            lock.unlock();
        }

        boolean hasBlockedTransaction()
        {
            return lock.getQueueLength() == 1;
        }
    }
}
