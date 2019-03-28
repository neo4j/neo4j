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

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.cache.idmapping.string.Workers;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TransactionRepresentationCommitProcessIT
{
    private static final String INDEX_NAME = "index";
    private static final int TOTAL_ACTIVE_THREADS = 6;

    @Rule
    public final DbmsRule db = new ImpermanentDbmsRule()
            .withSetting( GraphDatabaseSettings.check_point_interval_time, "10ms" );

    @Test( timeout = 15000 )
    public void commitDuringContinuousCheckpointing() throws Exception
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
                            Node node = db.createNode();
                            tx.success();
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

        CountsTracker counts = getDependency( RecordStorageEngine.class ).testAccessCountsStore();
        assertThat( "Count store should be rotated once at least", counts.txId(), greaterThan( 0L ) );

        long lastRotationTx = getDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
        TransactionIdStore txIdStore = getDependency( TransactionIdStore.class );
        assertEquals( "NeoStore last closed transaction id should be equal last count store rotation transaction id.",
                txIdStore.getLastClosedTransactionId(), lastRotationTx );
        assertEquals( "Last closed transaction should be last rotated tx in count store",
                txIdStore.getLastClosedTransactionId(), counts.txId() );
    }

    private <T> T getDependency( Class<T> clazz )
    {
        return db.getDependencyResolver().resolveDependency( clazz );
    }
}
