/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.core;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;

public class TestReadOnlyNeo4j
{
    private static final File PATH = new File( "read-only" );
    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void testSimple()
    {
        DbRepresentation someData = createSomeData();
        GraphDatabaseService readGraphDb = new TestGraphDatabaseFactory().setFileSystem( fs.get() )
                .newImpermanentDatabaseBuilder( PATH )
                .setConfig( GraphDatabaseSettings.read_only, Settings.TRUE )
                .newGraphDatabase();
        assertEquals( someData, DbRepresentation.of( readGraphDb ) );

        try (Transaction tx = readGraphDb.beginTx())
        {
            readGraphDb.createNode();

            tx.success();
        }
        catch ( TransactionFailureException e )
        {
            // good
            assertThat(e.getCause(), instanceOf( ReadOnlyDbException.class ));
        }
        readGraphDb.shutdown();
    }

    private DbRepresentation createSomeData()
    {
        DynamicRelationshipType type = withName( "KNOWS" );
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fs.get() ).newImpermanentDatabase( PATH );
        try ( Transaction tx = db.beginTx() )
        {
            Node prevNode = db.createNode();
            for ( int i = 0; i < 100; i++ )
            {
                Node node = db.createNode();
                Relationship rel = prevNode.createRelationshipTo( node, type );
                node.setProperty( "someKey" + i % 10, i % 15 );
                rel.setProperty( "since", System.currentTimeMillis() );
            }
            tx.success();
        }
        DbRepresentation result = DbRepresentation.of( db );
        db.shutdown();
        return result;
    }

    @Test
    public void testReadOnlyOperationsAndNoTransaction()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fs.get() ).newImpermanentDatabase(
                PATH );

        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        Relationship rel = node1.createRelationshipTo( node2, withName( "TEST" ) );
        node1.setProperty( "key1", "value1" );
        rel.setProperty( "key1", "value1" );
        tx.success();
        tx.close();

        // make sure write operations still throw exception
        try
        {
            db.createNode();
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            node1.createRelationshipTo( node2, withName( "TEST2" ) );
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }
        try
        {
            node1.setProperty( "key1", "value2" );
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }

        try
        {
            rel.removeProperty( "key1" );
            fail( "Write operation and no transaction should throw exception" );
        }
        catch ( NotInTransactionException e )
        { // good
        }


        Transaction transaction = db.beginTx();
        assertEquals( node1, db.getNodeById( node1.getId() ) );
        assertEquals( node2, db.getNodeById( node2.getId() ) );
        assertEquals( rel, db.getRelationshipById( rel.getId() ) );

        assertThat( node1, inTx( db, hasProperty( "key1" ).withValue( "value1" ) ) );
        Relationship loadedRel = node1.getSingleRelationship(
                DynamicRelationshipType.withName( "TEST" ), Direction.OUTGOING );
        assertEquals( rel, loadedRel );
        assertThat(loadedRel, inTx(db, hasProperty( "key1" ).withValue( "value1" )));
        transaction.close();
        db.shutdown();
    }
}
