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
package org.neo4j.kernel.impl.nioneo.store;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DefaultGraphDatabaseDependencies;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.logging.DevNullLoggingService;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ProduceUncleanStore
{
    public static void main( String[] args ) throws Exception
    {
        String storeDir = args[0];
        boolean setGraphProperty = args.length > 1 ? Boolean.parseBoolean( args[1] ) : false;
        GraphDatabaseService db = new EmbeddedGraphDatabase(
                storeDir,
                stringMap(),
                new DefaultGraphDatabaseDependencies( DevNullLoggingService.DEV_NULL ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", "Something" );
            if ( setGraphProperty )
            {
                //noinspection deprecation
                ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( NodeManager.class )
                        .getGraphProperties().setProperty( "prop", "Some value" );
            }
            tx.success();
        }
        System.exit( 0 );
    }
}
