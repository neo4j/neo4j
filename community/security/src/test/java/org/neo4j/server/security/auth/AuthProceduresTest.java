/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.security.auth;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.test.TestGraphDatabaseBuilder;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;

public class AuthProceduresTest extends KernelIntegrationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldFailWhenDeprecatedChangePasswordWithStaticAccessModeInDbmsMode() throws Throwable
    {
        // Given
        Object[] inputArray = new Object[1];
        inputArray[0] = "newPassword";

        // Then
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Anonymous cannot change password" );

        // When
        dbmsOperations()
                .procedureCallDbms( procedureName( "dbms", "changePassword" ), inputArray, AnonymousContext.none() );
    }

    @Test
    public void shouldFailWhenChangePasswordWithStaticAccessModeInDbmsMode() throws Throwable
    {
        // Given
        Object[] inputArray = new Object[1];
        inputArray[0] = "newPassword";

        // Then
        exception.expect( ProcedureException.class );
        exception.expectMessage( "Anonymous cannot change password" );

        // When
        dbmsOperations().procedureCallDbms( procedureName( "dbms", "security", "changePassword" ), inputArray, AnonymousContext.none() );
    }

    @Override
    protected TestGraphDatabaseBuilder configure( TestGraphDatabaseBuilder graphDatabaseBuilder )
    {
        graphDatabaseBuilder.setConfig( GraphDatabaseSettings.auth_enabled, "true" );
        return graphDatabaseBuilder;
    }
}
