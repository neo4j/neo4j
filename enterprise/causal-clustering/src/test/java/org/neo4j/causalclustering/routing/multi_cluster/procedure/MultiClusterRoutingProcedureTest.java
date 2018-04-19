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
package org.neo4j.causalclustering.routing.multi_cluster.procedure;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;

public class MultiClusterRoutingProcedureTest
{

    @Test
    public void subClusterRoutingProcedureShouldHaveCorrectSignature()
    {
        GetRoutersForDatabaseProcedure proc = new GetRoutersForDatabaseProcedure( null, Config.defaults() );

        ProcedureSignature procSig = proc.signature();

        List<FieldSignature> input = Collections.singletonList( FieldSignature.inputField( "database", Neo4jTypes.NTString ) );
        List<FieldSignature> output = Arrays.asList(
                FieldSignature.outputField( "ttl", Neo4jTypes.NTInteger ),
                FieldSignature.outputField( "routers", Neo4jTypes.NTList( Neo4jTypes.NTMap ) ) );

        assertEquals( "The input signature of the GetRoutersForDatabaseProcedure should not change.", procSig.inputSignature(), input );

        assertEquals( "The output signature of the GetRoutersForDatabaseProcedure should not change.", procSig.outputSignature(), output );
    }

    @Test
    public void superClusterRoutingProcedureShouldHaveCorrectSignature()
    {
        GetRoutersForAllDatabasesProcedure proc = new GetRoutersForAllDatabasesProcedure( null, Config.defaults() );

        ProcedureSignature procSig = proc.signature();

        List<FieldSignature> output = Arrays.asList(
                FieldSignature.outputField( "ttl", Neo4jTypes.NTInteger ),
                FieldSignature.outputField( "routers", Neo4jTypes.NTList( Neo4jTypes.NTMap ) ) );

        assertEquals( "The output signature of the GetRoutersForAllDatabasesProcedure should not change.", procSig.outputSignature(), output );
    }
}
