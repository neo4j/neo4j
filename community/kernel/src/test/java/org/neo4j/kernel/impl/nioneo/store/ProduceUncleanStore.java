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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.test.ProcessStreamHandler;

public class ProduceUncleanStore
{
    public static void main( String[] args ) throws Exception
    {
        String storeDir = args[0];
        boolean setGraphProperty = args.length > 1 ? Boolean.parseBoolean( args[1] ) : false;
        GraphDatabaseService db = new EmbeddedGraphDatabase( storeDir )
        {
            @Override
            protected Logging createLogging()
            {
                // Create a dev/null logging service due there being a locking problem
                // on windows (this class being run as a separate JVM from another test).
                // TODO investigate.
                return new DevNullLoggingService();
            }
        };
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Something" );
        if ( setGraphProperty ) ((GraphDatabaseAPI)db).getNodeManager().getGraphProperties().setProperty( "prop", "Some value" );
        tx.success();
        tx.finish();
        System.exit( 0 );
    }

    public static void atPath( File path ) throws Exception
    {
        Process process = Runtime.getRuntime()
            .exec( new String[]{
                "java", "-cp", System.getProperty( "java.class.path" ),
                ProduceUncleanStore.class.getName(), path.getAbsolutePath()
            } );
        int ret = new ProcessStreamHandler(process, true).waitForResult();
        assertEquals( "ProduceUncleanStore terminated unsuccessfully", 0, ret );
    }
}
