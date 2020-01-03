/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
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

        cleanTheDatabase( server.getDatabaseService().getDatabase() );
    }

    public static void cleanTheDatabase( GraphDatabaseAPI db )
    {
        new Transactor( db, new DeleteAllData( db ), 10 ).execute();
        new Transactor( db, new DeleteAllSchema(), 10 ).execute();
    }

    public static NeoServer createNonPersistentServer() throws IOException
    {
        return createServer( CommunityServerBuilder.server(), false, null );
    }

    public static NeoServer createReadOnlyServer( File path ) throws IOException
    {
        // Start writable server to create all store files needed
        CommunityServerBuilder builder = CommunityServerBuilder.server();
        builder.withProperty( "dbms.connector.bolt.listen_address", ":0" );
        createServer( builder, true, path ).stop();
        // Then start server in read only mode
        builder.withProperty( GraphDatabaseSettings.read_only.name(), "true" );
        return createServer( builder, true, path );
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
        public void doWork( Transaction transaction )
        {
            deleteAllNodesAndRelationships( transaction );
        }

        private void deleteAllNodesAndRelationships( Transaction tx )
        {
            Iterable<Node> allNodes = tx.getAllNodes();
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
    }

    private static class DeleteAllSchema implements UnitOfWork
    {
        @Override
        public void doWork( Transaction transaction )
        {
            deleteAllIndexRules( transaction );
            deleteAllConstraints( transaction );
        }

        private void deleteAllIndexRules( Transaction transaction )
        {
            for ( IndexDefinition index : transaction.schema().getIndexes() )
            {
                if ( !index.isConstraintIndex() )
                {
                    index.drop();
                }
            }
        }

        private void deleteAllConstraints( Transaction transaction )
        {
            for ( ConstraintDefinition constraint : transaction.schema().getConstraints() )
            {
                constraint.drop();
            }
        }
    }
}
