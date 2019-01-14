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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertTrue;

public class ShutdownOnIndexUpdateIT
{
    @Rule
    public DatabaseRule database = new ImpermanentDatabaseRule();

    private static final String UNIQUE_PROPERTY_NAME = "uniquePropertyName";
    private static final AtomicLong indexProvider = new AtomicLong();
    private static Label constraintIndexLabel = Label.label( "ConstraintIndexLabel" );

    @Test
    public void shutdownWhileFinishingTransactionWithIndexUpdates()
    {
        createConstraint( database );
        waitIndexesOnline( database );

        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( constraintIndexLabel );
            node.setProperty( UNIQUE_PROPERTY_NAME, indexProvider.getAndIncrement() );

            DependencyResolver dependencyResolver = database.getDependencyResolver();
            NeoStoreDataSource dataSource = dependencyResolver.resolveDependency( NeoStoreDataSource.class );
            LifeSupport dataSourceLife = dataSource.getLife();
            TransactionCloseListener closeListener = new TransactionCloseListener( transaction );
            dataSourceLife.addLifecycleListener( closeListener );
            dataSource.stop();

            assertTrue( "Transaction should be closed and no exception should be thrown.",
                    closeListener.isTransactionClosed() );
        }
    }

    private void waitIndexesOnline( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
        }
    }

    private void createConstraint( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Schema schema = database.schema();
            schema.constraintFor( constraintIndexLabel ).assertPropertyIsUnique( UNIQUE_PROPERTY_NAME ).create();
            transaction.success();
        }
    }

    private static class TransactionCloseListener implements LifecycleListener
    {
        private final Transaction transaction;
        private boolean transactionClosed;

        TransactionCloseListener( Transaction transaction )
        {
            this.transaction = transaction;
        }

        @Override
        public void notifyStatusChanged( Object instance, LifecycleStatus from, LifecycleStatus to )
        {
            if ( (LifecycleStatus.STOPPED == to) && (instance instanceof RecordStorageEngine) )
            {
                transaction.success();
                transaction.close();
                transactionClosed = true;
            }
        }

        boolean isTransactionClosed()
        {
            return transactionClosed;
        }
    }
}
