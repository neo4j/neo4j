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

import java.io.File;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.logging.LogProvider;

public final class WebContainerHelper
{
    private WebContainerHelper()
    {
    }

    public static void cleanTheDatabase( final TestWebContainer testWebContainer )
    {
        if ( testWebContainer == null )
        {
            return;
        }

        rollbackAllOpenTransactions( testWebContainer );

        cleanTheDatabase( testWebContainer.getDefaultDatabase() );
    }

    public static void cleanTheDatabase( GraphDatabaseService db )
    {
        new Transactor( db, new DeleteAllData(), 10 ).execute();
        new Transactor( db, new DeleteAllSchema(), 10 ).execute();
    }

    public static TestWebContainer createNonPersistentContainer() throws Exception
    {
        return createContainer( CommunityWebContainerBuilder.builder(), false, null );
    }

    public static TestWebContainer createReadOnlyContainer( File path ) throws Exception
    {
        CommunityWebContainerBuilder builder = CommunityWebContainerBuilder.builder();
        builder.withProperty( "dbms.connector.bolt.listen_address", ":0" );
        createContainer( builder, true, path ).shutdown();
        builder.withProperty( GraphDatabaseSettings.read_only.name(), "true" );
        return createContainer( builder, true, path );
    }

    public static TestWebContainer createNonPersistentContainer( LogProvider logProvider ) throws Exception
    {
        return createContainer( CommunityWebContainerBuilder.builder( logProvider ), false, null );
    }

    public static TestWebContainer createNonPersistentContainer( CommunityWebContainerBuilder builder ) throws Exception
    {
        return createContainer( builder, false, null );
    }

    private static TestWebContainer createContainer( CommunityWebContainerBuilder builder, boolean persistent, File path ) throws Exception
    {
        if ( persistent )
        {
            builder = builder.persistent();
        }
        builder.onRandomPorts();
        return builder
              .usingDataDir( path != null ? path.getAbsolutePath() : null )
              .build();
    }

    private static void rollbackAllOpenTransactions( TestWebContainer testWebContainer )
    {
        testWebContainer.getTransactionRegistry().rollbackAllSuspendedTransactions();
    }

    private static class DeleteAllData implements UnitOfWork
    {
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
