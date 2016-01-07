/*
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.storageengine.api.procedure.ProcedureSignature;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.storageengine.api.Neo4jTypes.NTString;
import static org.neo4j.storageengine.api.procedure.ProcedureSignature.procedureSignature;

public class ProceduresKernelIT extends KernelIntegrationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final ProcedureSignature signature = procedureSignature( "example", "exampleProc" )
            .in( "name", NTString )
            .out( "name", NTString ).build();

    @Test
    @Ignore("Pending implementation")
    public void shouldGetProcedureByName() throws Throwable
    {
        // When
        ProcedureSignature found = readOperationsInNewTransaction()
                .procedureGet( new ProcedureSignature.ProcedureName( new String[]{"example"}, "exampleProc" ) );

        // Then
        assertThat( found, equalTo( found ) );
    }
}
