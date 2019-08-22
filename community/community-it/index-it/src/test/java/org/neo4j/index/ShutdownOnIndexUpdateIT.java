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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ImpermanentDbmsExtension
class ShutdownOnIndexUpdateIT
{
    @Inject
    private GraphDatabaseAPI db;

    private static final String UNIQUE_PROPERTY_NAME = "uniquePropertyName";
    private static final AtomicLong indexProvider = new AtomicLong();
    private static final Label CONSTRAINT_INDEX_LABEL = Label.label( "ConstraintIndexLabel" );

    @Test
    void shutdownWhileFinishingTransactionWithIndexUpdates()
    {
        createConstraint( db );
        waitIndexesOnline( db );

        try ( Transaction transaction = db.beginTx() )
        {
            Node node = db.createNode( CONSTRAINT_INDEX_LABEL );
            node.setProperty( UNIQUE_PROPERTY_NAME, indexProvider.getAndIncrement() );

            DependencyResolver dependencyResolver = db.getDependencyResolver();
            Database dataSource = dependencyResolver.resolveDependency( Database.class );
            LifeSupport dataSourceLife = dataSource.getLife();
            TransactionCloseListener closeListener = new TransactionCloseListener( transaction );
            dataSourceLife.addLifecycleListener( closeListener );
            dataSource.stop();

            assertTrue( closeListener.isTransactionClosed(), "Transaction should be closed and no exception should be thrown." );
        }
    }

    private static void waitIndexesOnline( GraphDatabaseService database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            database.schema().awaitIndexesOnline( 5, TimeUnit.MINUTES );
        }
    }

    private static void createConstraint( GraphDatabaseService database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Schema schema = database.schema();
            schema.constraintFor( CONSTRAINT_INDEX_LABEL ).assertPropertyIsUnique( UNIQUE_PROPERTY_NAME ).create();
            transaction.commit();
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
                transaction.commit();
                transactionClosed = true;
            }
        }

        boolean isTransactionClosed()
        {
            return transactionClosed;
        }
    }
}
