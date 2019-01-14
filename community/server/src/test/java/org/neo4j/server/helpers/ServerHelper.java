/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.NeoServer;

public class ServerHelper
{

    private ServerHelper()
    {
    }

    public static void cleanTheDatabase( final NeoServer server )
    {
        if ( server == null )
        {
            return;
        }

        rollbackAllOpenTransactions( server );

        cleanTheDatabase( server.getDatabase().getGraph() );

        removeLogs( server );
    }

    public static void cleanTheDatabase( GraphDatabaseAPI db )
    {
        new Transactor( db, new DeleteAllData( db ), 10 ).execute();
        new Transactor( db, new DeleteAllSchema( db ), 10 ).execute();
    }

    private static void removeLogs( NeoServer server )
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

    public static NeoServer createNonPersistentServer() throws IOException
    {
        return createServer( CommunityServerBuilder.server(), false, null );
    }

    public static NeoServer createNonPersistentReadOnlyServer() throws IOException
    {
        CommunityServerBuilder builder = CommunityServerBuilder.server();
        builder.withProperty( GraphDatabaseSettings.read_only.name(), "true" );
        return createServer( builder, false, null );
    }

    public static NeoServer createNonPersistentServer( LogProvider logProvider ) throws IOException
    {
        return createServer( CommunityServerBuilder.server( logProvider ), false, null );
    }

    public static NeoServer createNonPersistentServer( CommunityServerBuilder builder ) throws IOException
    {
        return createServer( builder, false, null );
    }

    private static NeoServer createServer( CommunityServerBuilder builder, boolean persistent, File path )
            throws IOException
    {
        if ( persistent )
        {
            builder = builder.persistent();
        }
        builder.onRandomPorts();
        NeoServer server = builder
                .usingDataDir( path != null ? path.getAbsolutePath() : null )
                .build();

        server.start();
        return server;
    }

    private static void rollbackAllOpenTransactions( NeoServer server )
    {
        server.getTransactionRegistry().rollbackAllSuspendedTransactions();
    }

    private static class DeleteAllData implements UnitOfWork
    {
        private final GraphDatabaseAPI db;

        DeleteAllData( GraphDatabaseAPI db )
        {
            this.db = db;
        }

        @Override
        public void doWork()
        {
            deleteAllNodesAndRelationships();
            deleteAllIndexes();
        }

        private void deleteAllNodesAndRelationships()
        {
            Iterable<Node> allNodes = db.getAllNodes();
            for ( Node n : allNodes )
            {
                Iterable<Relationship> relationships = n.getRelationships();
                for ( Relationship rel : relationships )
                {
                    rel.delete();
                }
                n.delete();
            }
        }

        private void deleteAllIndexes()
        {
            IndexManager indexManager = db.index();

            for ( String indexName : indexManager.nodeIndexNames() )
            {
                try
                {
                    db.index()
                      .forNodes( indexName )
                      .delete();
                }
                catch ( UnsupportedOperationException e )
                {
                    // Encountered a read-only index.
                }
            }

            for ( String indexName : indexManager.relationshipIndexNames() )
            {
                try
                {
                    db.index()
                      .forRelationships( indexName )
                      .delete();
                }
                catch ( UnsupportedOperationException e )
                {
                    // Encountered a read-only index.
                }
            }

            for ( String k : indexManager.getNodeAutoIndexer().getAutoIndexedProperties() )
            {
                indexManager.getNodeAutoIndexer().stopAutoIndexingProperty( k );
            }
            indexManager.getNodeAutoIndexer().setEnabled( false );

            for ( String k : indexManager.getRelationshipAutoIndexer().getAutoIndexedProperties() )
            {
                indexManager.getRelationshipAutoIndexer().stopAutoIndexingProperty( k );
            }
            indexManager.getRelationshipAutoIndexer().setEnabled( false );
        }
    }

    private static class DeleteAllSchema implements UnitOfWork
    {
        private final GraphDatabaseAPI db;

        DeleteAllSchema( GraphDatabaseAPI db )
        {
            this.db = db;
        }

        @Override
        public void doWork()
        {
            deleteAllIndexRules();
            deleteAllConstraints();
        }

        private void deleteAllIndexRules()
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                if ( !index.isConstraintIndex() )
                {
                    index.drop();
                }
            }
        }

        private void deleteAllConstraints()
        {
            for ( ConstraintDefinition constraint : db.schema().getConstraints() )
            {
                constraint.drop();
            }
        }
    }
}
