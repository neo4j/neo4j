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

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.fs.FileUtils.deleteFile;

@ExtendWith( TestDirectoryExtension.class )
class IndexSamplingIntegrationTest
{
    @Inject
    private TestDirectory testDirectory;

    private final Label label = Label.label( "Person" );
    private final String property = "name";
    private final long nodes = 1000;
    private final String[] names = {"Neo4j", "Neo", "Graph", "Apa"};

    @Test
    void shouldSampleNotUniqueIndex() throws Throwable
    {
        GraphDatabaseService db = null;
        DatabaseManagementService managementService = null;
        long deletedNodes = 0;
        try
        {
            // Given
            managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
            db = managementService.database( DEFAULT_DATABASE_NAME );
            IndexDefinition indexDefinition;
            try ( Transaction tx = db.beginTx() )
            {
                indexDefinition = db.schema().indexFor( label ).on( property ).create();
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    db.createNode( label ).setProperty( property, names[i % names.length] );
                    tx.success();
                }

            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < (nodes / 10) ; i++ )
                {
                    try ( ResourceIterator<Node> nodes = db.findNodes( label, property, names[i % names.length] ) )
                    {
                        nodes.next().delete();
                    }
                    deletedNodes++;
                    tx.success();
                }
            }
        }
        finally
        {
            if ( managementService != null )
            {
                managementService.shutdown();
            }
        }

        // When
        triggerIndexResamplingOnNextStartup();

        // Then

        // lucene will consider also the delete nodes, native won't
        DoubleLongRegister register = fetchIndexSamplingValues( db );
        assertEquals( names.length, register.readFirst() );
        MatcherAssert.assertThat( register.readSecond(), allOf( greaterThanOrEqualTo( nodes - deletedNodes ), lessThanOrEqualTo( nodes ) ) );

        // but regardless, the deleted nodes should not be considered in the index size value
        DoubleLongRegister indexSizeRegister = fetchIndexSizeValues( db );
        assertEquals( 0, indexSizeRegister.readFirst() );
        assertEquals( nodes - deletedNodes, indexSizeRegister.readSecond() );
    }

    @Test
    void shouldSampleUniqueIndex() throws Throwable
    {
        GraphDatabaseService db = null;
        DatabaseManagementService managementService = null;
        long deletedNodes = 0;
        try
        {
            // Given
            managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
            db = managementService.database( DEFAULT_DATABASE_NAME );
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( label ).assertPropertyIsUnique( property ).create();
                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    db.createNode( label ).setProperty( property, "" + i );
                    tx.success();
                }
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    if ( i % 10 == 0 )
                    {
                        deletedNodes++;
                        db.findNode( label, property, "" + i ).delete();
                        tx.success();
                    }
                }
            }
        }
        finally
        {
            if ( db != null )
            {
                managementService.shutdown();
            }
        }

        // When
        triggerIndexResamplingOnNextStartup();

        // Then
        DoubleLongRegister indexSampleRegister = fetchIndexSamplingValues( db );
        assertEquals( nodes - deletedNodes, indexSampleRegister.readFirst() );
        assertEquals( nodes - deletedNodes, indexSampleRegister.readSecond() );

        DoubleLongRegister indexSizeRegister = fetchIndexSizeValues( db );
        assertEquals( 0, indexSizeRegister.readFirst() );
        assertEquals( nodes - deletedNodes, indexSizeRegister.readSecond() );
    }

    private IndexReference indexId( org.neo4j.internal.kernel.api.Transaction tx )
    {
        int labelId = tx.tokenRead().nodeLabel( label.name() );
        int propertyKeyId = tx.tokenRead().propertyKey( property );
        return tx.schemaRead().index( labelId, propertyKeyId );
    }

    private DoubleLongRegister fetchIndexSamplingValues( GraphDatabaseService db ) throws IndexNotFoundKernelException, TransactionFailureException
    {
        DatabaseManagementService managementService = null;
        try
        {
            // Then
            managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
            db = managementService.database( DEFAULT_DATABASE_NAME );
            @SuppressWarnings( "deprecation" )
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            Kernel kernel = api.getDependencyResolver().resolveDependency( Kernel.class );
            try ( org.neo4j.internal.kernel.api.Transaction tx = kernel.beginTransaction( explicit, AUTH_DISABLED ) )
            {
                return tx.schemaRead().indexSample( indexId( tx ), Registers.newDoubleLongRegister() );
            }
        }
        finally
        {
            if ( managementService != null )
            {
                managementService.shutdown();
            }
        }
    }

    private DoubleLongRegister fetchIndexSizeValues( GraphDatabaseService db ) throws IndexNotFoundKernelException, TransactionFailureException
    {
        DatabaseManagementService managementService = null;
        try
        {
            // Then
            managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
            db = managementService.database( DEFAULT_DATABASE_NAME );
            @SuppressWarnings( "deprecation" )
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            Kernel kernel = api.getDependencyResolver().resolveDependency( Kernel.class );
            try ( org.neo4j.internal.kernel.api.Transaction tx = kernel.beginTransaction( explicit, AUTH_DISABLED ) )
            {
                return tx.schemaRead().indexUpdatesAndSize( indexId( tx ), Registers.newDoubleLongRegister() );
            }
        }
        finally
        {
            if ( managementService != null )
            {
                managementService.shutdown();
            }
        }
    }

    private void triggerIndexResamplingOnNextStartup()
    {
        // Trigger index resampling on next at startup
        deleteFile( testDirectory.databaseLayout().countStoreA() );
        deleteFile( testDirectory.databaseLayout().countStoreB() );
    }
}
