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
package org.neo4j.harness;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

public class MyCoreAPI
{
    private final GraphDatabaseAPI graph;
    private final ThreadToStatementContextBridge txBridge;
    private final Log log;

    public MyCoreAPI( GraphDatabaseAPI graph, ThreadToStatementContextBridge txBridge, Log log )
    {
        this.graph = graph;
        this.txBridge = txBridge;
        this.log = log;
    }

    public long makeNode( String label ) throws ProcedureException
    {
        long result;
        try ( Transaction tx = graph.beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.write() );
                Statement statement = this.txBridge.get() )
        {
            long nodeId = statement.dataWriteOperations().nodeCreate();
            int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( label );
            statement.dataWriteOperations().nodeAddLabel( nodeId, labelId );
            result = nodeId;
            tx.success();
        }
        catch ( Exception e )
        {
            log.error( "Failed to create node: " + e.getMessage() );
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                    "Failed to create node: " + e.getMessage(), e );
        }
        return result;
    }

    public long countNodes()
    {
        long result;
        try ( Transaction tx = graph.beginTransaction( KernelTransaction.Type.explicit, AnonymousContext.read() );
                Statement statement = this.txBridge.get() )
        {
            result = statement.readOperations().countsForNode( -1 );
            tx.success();
        }
        return result;
    }
}
