/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.files;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.nativeimpl.AbsentNativeAccess;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;

@DbmsExtension( configurationCallback = "configure" )
class DatabasePanicIT
{
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private Config config;
    @Inject
    private DatabaseHealth databaseHealth;
    @Inject
    private Monitors monitors;
    private FailingNativeAccess nativeAccess;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        Dependencies dependencies = new Dependencies();
        nativeAccess = new FailingNativeAccess();
        dependencies.satisfyDependency( nativeAccess );
        builder.setExternalDependencies( dependencies );
    }

    @Test
    void panicOnLogRotationFailure()
    {
        long rotationThreshold = ByteUnit.kibiBytes( 128 );
        assertDoesNotThrow( () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    transaction.createNode();
                }
                transaction.commit();
            }
        } );

        config.setDynamic( logical_log_rotation_threshold, rotationThreshold, "test" );
        nativeAccess.startFailing();

        assertThrows(Exception.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                Node node = transaction.createNode();
                node.setProperty( "a", RandomStringUtils.randomAscii( (int) (rotationThreshold + 100) ) );
                transaction.commit();
            }
        } );

        // write transaction is failing
        assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.createNode();
                transaction.commit();
            }
        } );
        // read transaction is failing
        assertThrows( TransactionFailureException.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.getNodeById( 1 );
            }
        } );
        assertFalse( databaseHealth.isHealthy() );
    }

    private static class FailingNativeAccess extends AbsentNativeAccess
    {
        private final AtomicBoolean fail = new AtomicBoolean();

        @Override
        public NativeCallResult tryPreallocateSpace( int fd, long bytes )
        {
            if ( fail.get() )
            {
                throw new RuntimeException( "Something really wrong." );
            }
            return super.tryPreallocateSpace( fd, bytes );
        }

        public void startFailing()
        {
            fail.set( true );
        }
    }
}
