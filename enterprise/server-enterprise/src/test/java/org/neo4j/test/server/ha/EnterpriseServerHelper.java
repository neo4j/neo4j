/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test.server.ha;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.server.enterprise.EnterpriseNeoServer;
import org.neo4j.server.enterprise.helpers.EnterpriseServerBuilder;
import org.neo4j.server.helpers.Transactor;
import org.neo4j.server.helpers.UnitOfWork;
import org.neo4j.tooling.GlobalGraphOperations;

public class EnterpriseServerHelper
{
    public static void cleanTheDatabase( final EnterpriseNeoServer server )
    {
        if ( server == null )
        {
            return;
        }

        new Transactor( server.getDatabase().getGraph(), new UnitOfWork()
        {

            @Override
            public void doWork()
            {
                deleteAllNodesAndRelationships( server );

                deleteAllIndexes( server );
            }

            private void deleteAllNodesAndRelationships( final EnterpriseNeoServer server )
            {
                Iterable<Node> allNodes = GlobalGraphOperations.at( server.getDatabase().getGraph() ).getAllNodes();
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

            private void deleteAllIndexes( final EnterpriseNeoServer server )
            {
                IndexManager indexManager = server.getDatabase().getGraph().index();
                
                for ( String indexName : indexManager.nodeIndexNames() )
                {
                	try{
	                    server.getDatabase().getGraph().index()
	                            .forNodes( indexName )
	                            .delete();
                	} catch(UnsupportedOperationException e) {
                		// Encountered a read-only index.
                	}
                }

                for ( String indexName : indexManager.relationshipIndexNames() )
                {
                	try {
	                    server.getDatabase().getGraph().index()
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

        removeLogs( server );
    }

    private static void removeLogs( EnterpriseNeoServer server )
    {
        File logDir = new File( server.getDatabase().getLocation() + File.separator + ".." + File.separator + "log" );
        try
        {
            FileUtils.deleteDirectory( logDir );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public static EnterpriseNeoServer createNonPersistentServer( File databaseDir ) throws IOException
    {
        return createServer( databaseDir, false );
    }

    private static EnterpriseNeoServer createServer( File databaseDir, boolean persistent ) throws IOException
    {
        EnterpriseServerBuilder builder = EnterpriseServerBuilder.server().usingDatabaseDir( databaseDir.getAbsolutePath() );
        configureHostname( builder );
        if ( persistent ) builder = (EnterpriseServerBuilder) builder.persistent();
        builder.withDefaultDatabaseTuning();
        EnterpriseNeoServer server = builder.build();
        server.start();
        return server;
    }

    private static void configureHostname( EnterpriseServerBuilder builder )
    {
        String hostName = System.getProperty( "neo-server.test.hostname" );
        if (StringUtils.isNotEmpty( hostName )) {
            builder.onHost( hostName );
        }
    }
}
