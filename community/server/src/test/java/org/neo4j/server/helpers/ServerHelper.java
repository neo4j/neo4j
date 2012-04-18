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
package org.neo4j.server.helpers;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.server.NeoServer;
import org.neo4j.tooling.GlobalGraphOperations;

public class ServerHelper
{
    public static void cleanTheDatabase( final NeoServer server )
    {
        if ( server == null )
        {
            return;
        }

        new Transactor( server.getDatabase().graph, new UnitOfWork()
        {

            @Override
            public void doWork()
            {
                deleteAllNodesAndRelationships( server );

                deleteAllIndexes( server );
            }

            private void deleteAllNodesAndRelationships( final NeoServer server )
            {
                Iterable<Node> allNodes = GlobalGraphOperations.at( server.getDatabase().graph ).getAllNodes();
                for ( Node n : allNodes )
                {
                    Iterable<Relationship> relationships = n.getRelationships();
                    for ( Relationship rel : relationships )
                    {
                        rel.delete();
                    }
                    if ( n.getId() != 0 )
                    { // Don't delete the reference node - tests depend on it
                      // :-(
                        n.delete();
                    }
                    else
                    { // Remove all state from the reference node instead
                        for ( String key : n.getPropertyKeys() )
                        {
                            n.removeProperty( key );
                        }
                    }
                }
            }

            private void deleteAllIndexes( final NeoServer server )
            {
                IndexManager indexManager = server.getDatabase().graph.index();
                
                for ( String indexName : indexManager.nodeIndexNames() )
                {
                	try{
	                    server.getDatabase().graph.index()
	                            .forNodes( indexName )
	                            .delete();
                	} catch(UnsupportedOperationException e) {
                		// Encountered a read-only index.
                	}
                }

                for ( String indexName : indexManager.relationshipIndexNames() )
                {
                	try {
	                    server.getDatabase().graph.index()
	                            .forRelationships( indexName )
	                            .delete();
                	} catch(UnsupportedOperationException e) {
                		// Encountered a read-only index.
                	}
                }
                
                for(String k : indexManager.getNodeAutoIndexer().getAutoIndexedProperties()) 
                {
                    indexManager.getNodeAutoIndexer().stopAutoIndexingProperty(k);
                }
                indexManager.getNodeAutoIndexer().setEnabled(false);
                
                for(String k : indexManager.getRelationshipAutoIndexer().getAutoIndexedProperties()) 
                {
                    indexManager.getRelationshipAutoIndexer().stopAutoIndexingProperty(k);
                }
                indexManager.getRelationshipAutoIndexer().setEnabled(false);
            }
        } ).execute();
    }
    
    public static NeoServer createServer() throws IOException
    {
        return createServer( false );
    }
    
    public static NeoServer createServer( boolean persistent ) throws IOException
    {
        ServerBuilder builder = ServerBuilder.server();
        configureHostname( builder );
        if ( persistent ) builder = builder.persistent();
        NeoServer server = builder.build();
        server.start();
        return server;
    }

    private static void configureHostname( ServerBuilder builder )
    {
        String hostName = System.getProperty( "neo-server.test.hostname" );
        if (StringUtils.isNotEmpty( hostName )) {
            builder.onHost( hostName );
        }
    }
}
