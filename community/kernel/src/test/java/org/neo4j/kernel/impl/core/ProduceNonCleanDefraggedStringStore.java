/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;
import org.junit.Ignore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.MyRelTypes;

@Ignore( "Not a test" )
public class ProduceNonCleanDefraggedStringStore
{
    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( args[ 0 ] );
        
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
        
        // Non-clean "shutdown"
        System.exit( 0 );
    }

    private static void delete( Node node )
    {
        for ( Relationship rel : node.getRelationships() )
            rel.delete();
        node.delete();
    }
}
