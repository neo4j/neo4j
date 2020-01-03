/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class IndexSamplingIntegrationTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private final Label label = Label.label( "Person" );
    private final String property = "name";
    private final long nodes = 1000;
    private final String[] names = {"Neo4j", "Neo", "Graph", "Apa"};

    @Test
    public void shouldSampleNotUniqueIndex() throws Throwable
    {
        GraphDatabaseService db = null;
        long deletedNodes = 0;
        try
        {
            // Given
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
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
            if ( db != null )
            {
                db.shutdown();
            }
        }

        // When
        triggerIndexResamplingOnNextStartup();

        // Then

        // lucene will consider also the delete nodes, native won't
        DoubleLongRegister register = fetchIndexSamplingValues( db );
        assertEquals( names.length, register.readFirst() );
        assertThat( register.readSecond(), allOf( greaterThanOrEqualTo( nodes - deletedNodes ), lessThanOrEqualTo( nodes ) ) );

        // but regardless, the deleted nodes should not be considered in the index size value
        DoubleLongRegister indexSizeRegister = fetchIndexSizeValues( db );
        assertEquals( 0, indexSizeRegister.readFirst() );
        assertEquals( nodes - deletedNodes, indexSizeRegister.readSecond() );
    }

    @Test
    public void shouldSampleUniqueIndex() throws Throwable
    {
        GraphDatabaseService db = null;
        long deletedNodes = 0;
        try
        {
            // Given
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
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
                db.shutdown();
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
        try
        {
            // Then
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
            @SuppressWarnings( "deprecation" )
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            try ( org.neo4j.internal.kernel.api.Transaction tx = api.getDependencyResolver().resolveDependency( Kernel.class )
                    .beginTransaction( explicit, AUTH_DISABLED ) )
            {
                return tx.schemaRead().indexSample( indexId( tx ), Registers.newDoubleLongRegister() );
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private DoubleLongRegister fetchIndexSizeValues( GraphDatabaseService db ) throws IndexNotFoundKernelException, TransactionFailureException
    {
        try
        {
            // Then
            db = new TestGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
            @SuppressWarnings( "deprecation" )
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            try ( org.neo4j.internal.kernel.api.Transaction tx = api.getDependencyResolver().resolveDependency( Kernel.class )
                    .beginTransaction( explicit, AUTH_DISABLED ) )
            {
                return tx.schemaRead().indexUpdatesAndSize( indexId( tx ), Registers.newDoubleLongRegister() );
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private void triggerIndexResamplingOnNextStartup()
    {
        // Trigger index resampling on next at startup
        FileUtils.deleteFile( testDirectory.databaseLayout().countStoreA() );
        FileUtils.deleteFile( testDirectory.databaseLayout().countStoreB() );
    }
}
