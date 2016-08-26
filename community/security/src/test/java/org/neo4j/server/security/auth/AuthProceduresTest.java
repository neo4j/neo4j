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
package org.neo4j.server.security.auth;

import org.junit.Test;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;

public class AuthProceduresTest extends KernelIntegrationTest
{
    @Test
    public void callDeprecatedChangePasswordWithAccessModeInDbmsMode() throws Throwable
    {
        // Given
        Object[] inputArray = new Object[1];
        inputArray[0] = "newPassword";
        AuthSubject authSubject = mock( AuthSubject.class );

        // When
        RawIterator<Object[], ProcedureException> stream = dbmsOperations( authSubject )
                .procedureCallDbms( procedureName( "dbms", "changePassword" ), inputArray );

        // Then
        verify( authSubject ).setPassword( (String) inputArray[0] );
        assertThat( asList( stream ), emptyIterable() );
    }

    @Test
    public void shouldFailWhenDeprecatedChangePasswordWithStaticAccessModeInDbmsMode() throws Throwable
    {
        try
        {
            // Given
            Object[] inputArray = new Object[1];
            inputArray[0] = "newPassword";

            // When
            dbmsOperations( AccessMode.Static.NONE ).procedureCallDbms( procedureName( "dbms", "changePassword" ), inputArray );
            fail( "Should have failed." );
        }
        catch ( Exception e )
        {
            // Then
            assertThat( e.getClass(), equalTo( ProcedureException.class ) );
        }
    }

    @Test
    public void callChangePasswordWithAccessModeInDbmsMode() throws Throwable
    {
        // Given
        Object[] inputArray = new Object[1];
        inputArray[0] = "newPassword";
        AuthSubject authSubject = mock( AuthSubject.class );

        // When
        RawIterator<Object[],ProcedureException> stream = dbmsOperations( authSubject )
                .procedureCallDbms( procedureName( "dbms", "security", "changePassword" ), inputArray );

        // Then
        verify( authSubject ).setPassword( (String) inputArray[0] );
        assertThat( asList( stream ), emptyIterable() );
    }

    @Test
    public void shouldFailWhenChangePasswordWithStaticAccessModeInDbmsMode() throws Throwable
    {
        try
        {
            // Given
            Object[] inputArray = new Object[1];
            inputArray[0] = "newPassword";

            // When
            dbmsOperations( AccessMode.Static.NONE )
                    .procedureCallDbms( procedureName( "dbms", "security", "changePassword" ), inputArray );
            fail( "Should have failed." );
        }
        catch ( Exception e )
        {
            // Then
            assertThat( e.getClass(), equalTo( ProcedureException.class ) );
        }
    }
}
