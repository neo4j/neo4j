/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;

public class ProduceUncleanStore
{
    public static void main( String[] args ) throws Exception
    {
        String storeDir = args[0];
        boolean setGraphProperty = args.length > 1 ? Boolean.parseBoolean( args[1] ) : false;
            GraphDatabaseService db = new EmbeddedGraphDatabase(
                    storeDir, new HashMap<String, String>(), Iterables.<KernelExtensionFactory<?>,
                    KernelExtensionFactory>cast( Service.load( KernelExtensionFactory.class ) ),
                    Service.load( CacheProvider.class ), Service.load( TransactionInterceptorProvider.class ) )
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
