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
package org.neo4j.kernel.impl.proc;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.api.proc.FieldSignature;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.UserFunctionSignature;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.api.proc.UserFunctionSignature.functionSignature;

public class UserFunctionSignatureTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private final UserFunctionSignature signature =
            functionSignature( "asd" ).in( "in", Neo4jTypes.NTAny ).out( Neo4jTypes.NTAny ).build();

    @Test
    public void inputSignatureShouldNotBeModifiable() throws Throwable
    {
        // Expect
        exception.expect( UnsupportedOperationException.class );

        // When
        signature.inputSignature().add( new FieldSignature( "in2", Neo4jTypes.NTAny ));
    }

    @Test
    public void toStringShouldMatchCypherSyntax() throws Throwable
    {
        // When
        String toStr = functionSignature( "org", "myProcedure" )
                .in( "in", Neo4jTypes.NTList( Neo4jTypes.NTString ) )
                .out( Neo4jTypes.NTNumber )
                .build()
                .toString();

        // Then
        assertEquals( "org.myProcedure(in :: LIST? OF STRING?) :: (NUMBER?)", toStr );
    }
}
