/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.cache.idmapping.string.Workers;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
class TransactionRepresentationCommitProcessIT
{
    private static final int TOTAL_ACTIVE_THREADS = 6;

    @Inject
    private GraphDatabaseAPI db;

    @ExtensionCallback
    static void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseSettings.check_point_interval_time, Duration.ofMillis( 10 ) );
    }

    @Test
    void commitDuringContinuousCheckpointing()
    {
        assertTimeoutPreemptively( ofSeconds( 15 ), () ->
        {
            final AtomicBoolean done = new AtomicBoolean();
            Workers<Runnable> workers = new Workers<>( getClass().getSimpleName() );
            for ( int i = 0; i < TOTAL_ACTIVE_THREADS; i++ )
            {
                workers.start( new Runnable()
                {
                    private final ThreadLocalRandom random = ThreadLocalRandom.current();

                    @Override
                    public void run()
                    {
                        while ( !done.get() )
                        {
                            try ( Transaction tx = db.beginTx() )
                            {
                                db.createNode();
                                tx.commit();
                            }
                            randomSleep();
                        }
                    }

                    private void randomSleep()
                    {
                        try
                        {
                            Thread.sleep( random.nextInt( 50 ) );
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
                        }
                    }
                } );
            }

            Thread.sleep( SECONDS.toMillis( 2 ) );
            done.set( true );
            workers.awaitAndThrowOnError();

            CountsTracker counts = (CountsTracker) getDependency( CountsAccessor.class );
            assertThat( "Count store should be rotated once at least", counts.txId(), greaterThan( 0L ) );

            long lastRotationTx = getDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
            TransactionIdStore txIdStore = getDependency( TransactionIdStore.class );
            assertEquals( txIdStore.getLastClosedTransactionId(), lastRotationTx,
                    "NeoStore last closed transaction id should be equal last count store rotation transaction id." );
            assertEquals( txIdStore.getLastClosedTransactionId(), counts.txId(), "Last closed transaction should be last rotated tx in count store" );
        } );
    }

    private <T> T getDependency( Class<T> clazz )
    {
        return db.getDependencyResolver().resolveDependency( clazz );
    }
}
