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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.impl.index.DummyIndexExtensionFactory;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Workers;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class TransactionRepresentationCommitProcessIT
{
    private static final String INDEX_NAME = "index";
    private static final int TOTAL_ACTIVE_THREADS = 6;

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule()
            .withSetting( GraphDatabaseSettings.check_point_interval_time, "10ms" );

    @Test( timeout = 15000 )
    public void commitDuringContinuousCheckpointing() throws Exception
    {
        final Index<Node> index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.index().forNodes( INDEX_NAME, stringMap(
                    IndexManager.PROVIDER, DummyIndexExtensionFactory.IDENTIFIER ) );
            tx.success();
        }

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
                            index.add( node, "key", node.getId() );
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

        NeoStores neoStores = getDependency(RecordStorageEngine.class).testAccessNeoStores();
        assertThat( "Count store should be rotated once at least", neoStores.getCounts().txId(), greaterThan( 0L ) );

        long lastRotationTx = getDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
        assertEquals( "NeoStore last closed transaction id should be equal last count store rotation transaction id.",
                neoStores.getMetaDataStore().getLastClosedTransactionId(), lastRotationTx );
        assertEquals( "Last closed transaction should be last rotated tx in count store",
                neoStores.getMetaDataStore().getLastClosedTransactionId(), neoStores.getCounts().txId() );
    }

    private <T> T getDependency( Class<T> clazz )
    {
        return db.getDependencyResolver().resolveDependency( clazz );
    }
}
