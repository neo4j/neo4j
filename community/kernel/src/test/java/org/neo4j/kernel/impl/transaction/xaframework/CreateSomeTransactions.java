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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.BatchTransaction;

import static org.neo4j.test.BatchTransaction.*;

public class CreateSomeTransactions
{
    public static void main( String[] args ) throws IOException
    {
        String sourceDir = args[0];
        boolean shutdown = Boolean.parseBoolean( args[1] );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( sourceDir ).setConfig( GraphDatabaseSettings.keep_logical_logs, GraphDatabaseSetting.TRUE ).newGraphDatabase();
        
        BatchTransaction tx = beginBatchTx( db );
        Node node = db.createNode();
        node.setProperty( "name", "First" );
        Node otherNode = db.createNode();
        node.createRelationshipTo( otherNode, MyRelTypes.TEST );
        tx.restart();
        db.getXaDataSourceManager().getNeoStoreDataSource().rotateLogicalLog();
        
        for ( int i = 0; i < 5; i++ )
        {
            db.createNode().setProperty( "type", i );
            tx.restart();
        }
        tx.finish();
        if ( shutdown ) db.shutdown();
        else System.exit( 1 );
    }
}
