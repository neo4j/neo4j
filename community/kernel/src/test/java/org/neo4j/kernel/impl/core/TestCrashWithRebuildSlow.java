/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.helpers.collection.IteratorUtil.count;

/**
 * Test for making sure that slow id generator rebuild is exercised and also a problem
 * @author Mattias Persson
 */
public class TestCrashWithRebuildSlow
{
    @Test
    public void crashAndRebuildSlowWithDynamicStringDeletions() throws Exception
    {
        String storeDir = "dir";
        final GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .setFileSystem( fs.get() ).newImpermanentDatabase( storeDir );
        produceNonCleanDefraggedStringStore( db );
        EphemeralFileSystemAbstraction snapshot = fs.snapshot( new Runnable()
        {
            @Override
            public void run()
            {
                db.shutdown();
            }
        } );
        
        // Recover with rebuild_idgenerators_fast=false
        assertNumberOfFreeIdsEquals( storeDir, snapshot, 0 );
        GraphDatabaseAPI newDb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( snapshot )
                .newImpermanentDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.rebuild_idgenerators_fast, Settings.FALSE )
                .newGraphDatabase();
        assertNumberOfFreeIdsEquals( storeDir, snapshot, 4 );

        Transaction transaction = newDb.beginTx();
        try
        {
            int nameCount = 0;
            int relCount = 0;
            for ( Node node : GlobalGraphOperations.at( newDb ).getAllNodes() )
            {
                if ( node.equals( newDb.getReferenceNode() ) )
                    continue;
                nameCount++;
                assertThat( node, inTx( newDb, hasProperty( "name" )  ) );
                relCount += count( node.getRelationships( Direction.OUTGOING ) );
            }
            
            assertEquals( 16, nameCount );
            assertEquals( 12, relCount );
        }
        finally
        {
            transaction.finish();
            newDb.shutdown();
        }
    }

    private void assertNumberOfFreeIdsEquals( String storeDir, EphemeralFileSystemAbstraction snapshot, int numberOfFreeIds )
    {
        assertEquals( 9/*header*/ + 8*numberOfFreeIds,
                snapshot.getFileSize( new File( storeDir, "neostore.propertystore.db.strings.id" ) ) );
    }

    private void produceNonCleanDefraggedStringStore( GraphDatabaseService db )
    {
        // Create some strings
        List<Node> nodes = new ArrayList<Node>();
        Transaction tx = db.beginTx();
        try
        {
            Node previous = null;
            for ( int i = 0; i < 20; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "name", "a looooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooong string" );
                nodes.add( node );
                if ( previous != null )
                    previous.createRelationshipTo( node, MyRelTypes.TEST );
                previous = node;
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        // Delete some of them, but leave some in between deletions
        tx = db.beginTx();
        try
        {
            delete( nodes.get( 5 ) );
            delete( nodes.get( 7 ) );
            delete( nodes.get( 8 ) );
            delete( nodes.get( 10 ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private static void delete( Node node )
    {
        for ( Relationship rel : node.getRelationships() )
            rel.delete();
        node.delete();
    }
    
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
