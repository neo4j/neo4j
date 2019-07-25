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
package org.neo4j.server.security.auth;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.values.AnyValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.kernel.api.ResourceManager.EMPTY_RESOURCE_MANAGER;
import static org.neo4j.values.storable.Values.stringValue;

class AuthProceduresTest extends KernelIntegrationTest
{
    @Test
    void shouldFailWhenChangePasswordWithStaticAccessModeInDbmsMode() throws Throwable
    {
        // Given
        AnyValue[] inputArray = new AnyValue[1];
        inputArray[0] = stringValue( "newPassword" );

        // When
        int procedureId = procs().procedureGet( procedureName( "dbms", "security", "changePassword" ) ).id();

        var e = assertThrows( ProcedureException.class, () -> dbmsOperations().procedureCallDbms( procedureId, inputArray, dependencyResolver,
            AnonymousContext.none().authorize( LoginContext.IdLookup.EMPTY, GraphDatabaseSettings.DEFAULT_DATABASE_NAME ),
            EMPTY_RESOURCE_MANAGER, valueMapper ) );

        assertThat( e.getMessage(), Matchers.equalTo( "Anonymous cannot change password" ) );
    }

    @Override
    protected TestDatabaseManagementServiceBuilder configure( TestDatabaseManagementServiceBuilder databaseManagementServiceBuilder )
    {
        return databaseManagementServiceBuilder.setConfig( GraphDatabaseSettings.auth_enabled, true );
    }
}
