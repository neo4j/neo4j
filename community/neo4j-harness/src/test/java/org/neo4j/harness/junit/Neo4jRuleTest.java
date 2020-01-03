/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.harness.junit;

import org.junit.jupiter.api.Test;
import org.junit.runners.model.Statement;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.runner.Description.createTestDescription;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Neo4jRuleTest
{
    @Test
    void shouldReturnHttpsUriWhenConfigured() throws Throwable
    {
        URI configuredHttpsUri = URI.create( "https://localhost:7473" );
        assertEquals( configuredHttpsUri, getHttpsUriFromNeo4jRule( configuredHttpsUri ) );
    }

    @Test
    void shouldThrowWhenHttpsUriNotConfigured() throws Throwable
    {
        assertThrows( IllegalStateException.class, () -> getHttpsUriFromNeo4jRule( null ) );
    }

    private static URI getHttpsUriFromNeo4jRule( URI configuredHttpsUri ) throws Throwable
    {
        ServerControls serverControls = mock( ServerControls.class );
        when( serverControls.httpsURI() ).thenReturn( Optional.ofNullable( configuredHttpsUri ) );
        TestServerBuilder serverBuilder = mock( TestServerBuilder.class );
        when( serverBuilder.newServer() ).thenReturn( serverControls );

        Neo4jRule rule = new Neo4jRule( serverBuilder );

        AtomicReference<URI> uriRef = new AtomicReference<>();
        Statement statement = rule.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                uriRef.set( rule.httpsURI() );
            }
        }, createTestDescription( Neo4jRuleTest.class, "test" ) );

        statement.evaluate();
        return uriRef.get();
    }
}
