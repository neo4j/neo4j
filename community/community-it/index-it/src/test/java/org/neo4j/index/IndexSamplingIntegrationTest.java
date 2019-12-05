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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.fs.FileUtils.deleteFile;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;

@Neo4jLayoutExtension
class IndexSamplingIntegrationTest
{
    @Inject
    private DatabaseLayout databaseLayout;

    private final Label label = Label.label( "Person" );
    private final String property = "name";
    private final String schemaName = "schema_name";
    private final long nodes = 1000;
    private final String[] names = {"Neo4j", "Neo", "Graph", "Apa"};

    @Test
    void shouldSampleNotUniqueIndex() throws Throwable
    {
        GraphDatabaseService db;
        DatabaseManagementService managementService = null;
        long deletedNodes = 0;
        try
        {
            // Given
            managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).build();
            db = managementService.database( DEFAULT_DATABASE_NAME );
            IndexDefinition indexDefinition;
            try ( Transaction tx = db.beginTx() )
            {
                indexDefinition = tx.schema().indexFor( label ).on( property ).withName( schemaName ).create();
                tx.commit();
            }

            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
                tx.commit();
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    tx.createNode( label ).setProperty( property, names[i % names.length] );
                }
                tx.commit();
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < (nodes / 10) ; i++ )
                {
                    try ( ResourceIterator<Node> nodes = tx.findNodes( label, property, names[i % names.length] ) )
                    {
                        nodes.next().delete();
                    }
                    deletedNodes++;
                }
                tx.commit();
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
        var indexSample = fetchIndexSamplingValues();
        assertEquals( names.length, indexSample.uniqueValues() );
        assertThat( indexSample.sampleSize() ).isGreaterThanOrEqualTo( nodes - deletedNodes ).isLessThanOrEqualTo( nodes );
        // but regardless, the deleted nodes should not be considered in the index size value
        assertEquals( 0, indexSample.updates() );
        assertEquals( nodes - deletedNodes, indexSample.indexSize() );
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
            managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).build();
            db = managementService.database( DEFAULT_DATABASE_NAME );
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().constraintFor( label ).assertPropertyIsUnique( property ).withName( schemaName ).create();
                tx.commit();
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    tx.createNode( label ).setProperty( property, "" + i );
                }
                tx.commit();
            }

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < nodes; i++ )
                {
                    if ( i % 10 == 0 )
                    {
                        deletedNodes++;
                        tx.findNode( label, property, "" + i ).delete();
                    }
                }
                tx.commit();
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
        var indexSample = fetchIndexSamplingValues();
        assertEquals( nodes - deletedNodes, indexSample.uniqueValues() );
        assertEquals( nodes - deletedNodes, indexSample.sampleSize() );
        assertEquals( 0, indexSample.updates() );
        assertEquals( nodes - deletedNodes, indexSample.indexSize() );
    }

    private IndexDescriptor indexId( KernelTransaction tx )
    {
        return tx.schemaRead().indexGetForName( schemaName );
    }

    private IndexSample fetchIndexSamplingValues() throws IndexNotFoundKernelException, TransactionFailureException
    {
        DatabaseManagementService managementService = null;
        try
        {
            // Then
            managementService = new TestDatabaseManagementServiceBuilder( databaseLayout ).build();
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            Kernel kernel = api.getDependencyResolver().resolveDependency( Kernel.class );
            try ( KernelTransaction tx = kernel.beginTransaction( EXPLICIT, AUTH_DISABLED ) )
            {
                return tx.schemaRead().indexSample( indexId( tx ) );
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
        deleteFile( databaseLayout.countStore() );
    }
}
