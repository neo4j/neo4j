/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.Map;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.cypher.javacompat.internal.ServerExecutionEngine;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.App;
import org.neo4j.shell.ShellException;

@Service.Implementation(App.class)
public class Using extends Start
{
    @Override
    protected ExecutionResult getResult( String query, Map<String, Object> parameters )
            throws ShellException, RemoteException, SystemException
    {
        GraphDatabaseAPI graphDatabaseAPI = getServer().getDb();
        ServerExecutionEngine engine = getEngine();
        if ( engine.isPeriodicCommit( query ) )
        {
            TransactionManager manager =
                graphDatabaseAPI.getDependencyResolver().resolveDependency( TransactionManager.class );
            Transaction tx = manager.suspend();
            try
            {
                return super.getResult( query, parameters );
            }
            finally
            {
                manager.resume( tx );
            }
        }
        else
        {
            return super.getResult( query, parameters );
        }
    }
}
