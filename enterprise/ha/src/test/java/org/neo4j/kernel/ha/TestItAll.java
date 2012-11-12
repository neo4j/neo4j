/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import static java.util.Arrays.asList;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

public class TestItAll
{
    private TargetDirectory PATH;
    private HighlyAvailableGraphDatabase[] databases;

    private enum Types implements RelationshipType
    {
        TEST;
    }

    @Before
    public void before() throws Exception
    {
        PATH = TargetDirectory.forTest( getClass() );
        PATH.cleanup();
    }

    @After
    public void after() throws Exception
    {
        for ( HighlyAvailableGraphDatabase database : databases )
        {
            database.shutdown();
        }
    }

    @Test
    public void slaveCreateNode() throws Exception
    {
        startDbs( 2 );
        int master = getMaster();
        createNode( (master + 1) % 2, "Slave node" );
        assertNodesExists( 0, "Slave node" );
        assertNodesExists( 1, "Slave node" );
    }

    private void startDbs( int count )
    {
        databases = new HighlyAvailableGraphDatabase[count];
        for ( int i = 0; i < count; i++ )
        {
            startDb( i );
        }
    }

    private HighlyAvailableGraphDatabase startDb( final int serverId )
    {

        HighlyAvailableGraphDatabase database = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( path( serverId ) )
                .setConfig( HaSettings.server_id, "" + serverId )
                .setConfig( HaSettings.ha_server, "127.0.0.1:" + (8001 + serverId) )
                .setConfig( HaSettings.cluster_server, "127.0.0.1:" + (5001 + serverId) )
                .setConfig( HaSettings.initial_hosts, "127.0.0.1:5001" )
                .newGraphDatabase();

        databases[serverId] = database;
        return database;
    }

    private String path( int serverId )
    {
        return PATH.directory( "" + serverId, false ).getAbsolutePath();
    }

    private void createNode( int id, String name )
    {
        GraphDatabaseService db = databases[id];
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        db.getReferenceNode().createRelationshipTo( node, Types.TEST );
        node.setProperty( "name", name );
        tx.success();
        tx.finish();
    }

    private int getMaster()
    {
        for ( int i = 0; i < databases.length; i++ )
        {
            if ( databases[i].isMaster() )
            {
                return i;
            }
        }

        {
            fail( "No master" );
        }
        return -1; // Unreachable code, fail will throw exception
    }

    private void assertNodesExists( int id, String... names )
    {
        GraphDatabaseService db = databases[id];
        Set<String> expectation = new HashSet<String>( asList( names ) );
        for ( Relationship rel : db.getReferenceNode().getRelationships() )
        {
            String name = (String) rel.getEndNode().getProperty( "name" );
            assertTrue( "Found unexpected name " + name, expectation.remove( name ) );
        }
        assertTrue( "Expected entries not encountered: " + expectation, expectation.isEmpty() );
    }
}
