/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import java.io.File;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class DoSomeTransactionsThenWait
{
    public static void main( String[] args ) throws Exception
    {
        String storeDir = args[0];
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        int count = Integer.parseInt( args[1] );
        for ( int i = 0; i < count; i++ )
        {
            Transaction tx = db.beginTx();
            try
            {
                db.createNode();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
        
        touch( storeDir, "done" );
        while ( true ) Thread.sleep( 1000 );
    }

    private static void touch( String storeDir, String name ) throws Exception
    {
        new File( storeDir, name ).createNewFile();
    }
}
