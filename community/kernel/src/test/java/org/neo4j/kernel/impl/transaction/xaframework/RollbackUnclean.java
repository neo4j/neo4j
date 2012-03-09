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
package org.neo4j.kernel.impl.transaction.xaframework;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class RollbackUnclean
{
    /*
     * Creates a damaged log file with dual start entries for the tx that is rolledback
     */
    public static void main( String[] args )
    {
        String storeDir = args[0];
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).newGraphDatabase();
        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        node1.createRelationshipTo( node2,
                DynamicRelationshipType.withName( "relType1" ) );
        tx.success();
        tx.finish();

        tx = db.beginTx();
        node1.delete();
        tx.success();
        try
        {
            // Will throw exception, causing the tx to be rolledback.
            tx.finish();
        }
        catch ( Exception nothingToSeeHereMoveAlong )
        {
            // InvalidRecordException coming, node1 has rels
        }
        /*
         *  The damage has already been done. The following just makes sure
         *  the corrupting tx is flushed to disk, since we will exit
         *  uncleanly.
         */
        tx = db.beginTx();
        node1.setProperty( "foo", "bar" );
        tx.success();
        tx.finish();
        // No shutdown
        System.exit( 0 );
    }
}
