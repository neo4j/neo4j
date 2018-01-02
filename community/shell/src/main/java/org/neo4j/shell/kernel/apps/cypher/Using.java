/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.shell.kernel.apps.cypher;

import java.rmi.RemoteException;

import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.shell.App;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

@Service.Implementation(App.class)
public class Using extends Start
{
    @Override
    protected Result getResult( String query, final Session session )
            throws ShellException, RemoteException, QueryExecutionKernelException
    {
        GraphDatabaseAPI graphDatabaseAPI = getServer().getDb();
        QueryExecutionEngine engine = getEngine();
        if ( engine.isPeriodicCommit( query ) )
        {
            ThreadToStatementContextBridge manager =
                graphDatabaseAPI.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
            TopLevelTransaction tx = manager.getTopLevelTransactionBoundToThisThread( true );
            manager.unbindTransactionFromCurrentThread();

            try
            {
                return super.getResult( query, session );
            }
            finally
            {
                manager.bindTransactionToCurrentThread( tx );
            }
        }
        else
        {
            return super.getResult( query, session );
        }
    }
}
