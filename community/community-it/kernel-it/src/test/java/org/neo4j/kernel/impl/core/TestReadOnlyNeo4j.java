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
package org.neo4j.kernel.impl.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

public class TestReadOnlyNeo4j
{
    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fs ).around( testDirectory );

    @Test
    public void testSimple()
    {
        DbRepresentation someData = createSomeData();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs.get() ) )
                .impermanent()
                .setConfig( GraphDatabaseSettings.read_only, Settings.TRUE )
                .build();
        GraphDatabaseService readGraphDb = managementService.database( DEFAULT_DATABASE_NAME );
        assertEquals( someData, DbRepresentation.of( readGraphDb ) );

        try ( Transaction tx = readGraphDb.beginTx() )
        {
            readGraphDb.createNode();

            tx.success();
        }
        catch ( WriteOperationsNotAllowedException e )
        {
            // good
        }
        managementService.shutdown();
    }

    private DbRepresentation createSomeData()
    {
        RelationshipType type = withName( "KNOWS" );
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs.get() ) )
                .impermanent()
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
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
        managementService.shutdown();
        return result;
    }

    @Test
    public void testReadOnlyOperationsAndNoTransaction()
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( fs.get() )
                .impermanent()
                .build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

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
        Relationship loadedRel = node1.getSingleRelationship( withName( "TEST" ), Direction.OUTGOING );
        assertEquals( rel, loadedRel );
        assertThat(loadedRel, inTx(db, hasProperty( "key1" ).withValue( "value1" )));
        transaction.close();
        managementService.shutdown();
    }
}
