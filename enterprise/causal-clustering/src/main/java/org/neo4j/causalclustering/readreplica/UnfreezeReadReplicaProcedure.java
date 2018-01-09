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
package org.neo4j.causalclustering.readreplica;

import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.QualifiedName;
import org.neo4j.logging.Log;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class UnfreezeReadReplicaProcedure extends CallableProcedure.BasicProcedure
{
    private static final String PROCEDURE_NAME = "unfreezeReadReplica";
    private static final String[] PROCEDURE_NAMESPACE = {"dbms", "cluster"};
    private static final String OUTPUT_NAME = "status";

    private CatchupPollingProcess catchupPollingProcess;
    private Log log;

    UnfreezeReadReplicaProcedure( CatchupPollingProcess catchupPollingProcess, Log log )
    {
        super( procedureSignature( new QualifiedName( PROCEDURE_NAMESPACE, PROCEDURE_NAME ) )
                .out( OUTPUT_NAME, Neo4jTypes.NTString )
                .description( "Unfreeze the current read replica. It will synchronize with other cluster members." )
                .build() );

        this.catchupPollingProcess = catchupPollingProcess;
        this.log = log;
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        try
        {
            catchupPollingProcess.start();
        }
        catch ( Throwable throwable )
        {
            log.error( "Failed to unfreeze read replica.", throwable );
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable, "Failed to unfreeze read replica. Check neo4j.log for details." );
        }
        return RawIterator.<Object[],ProcedureException>of( new Object[]{"unfrozen"} );
    }
}
