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
package org.neo4j.harness.junit.rule;

import org.junit.jupiter.api.Test;
import org.junit.runners.model.Statement;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.harness.internal.InProcessNeo4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private static URI getHttpsUriFromNeo4jRule( URI configuredHttpsUri ) throws Throwable
    {
        InProcessNeo4j neo4J = mock( InProcessNeo4j.class );
        when( neo4J.httpsURI() ).thenReturn( configuredHttpsUri );
        Neo4jBuilder serverBuilder = mock( Neo4jBuilder.class );
        when( serverBuilder.build() ).thenReturn( neo4J );

        Neo4jRule rule = new Neo4jRule( serverBuilder );

        AtomicReference<URI> uriRef = new AtomicReference<>();
        Statement statement = rule.apply( new Statement()
        {
            @Override
            public void evaluate()
            {
                uriRef.set( rule.httpsURI() );
            }
        }, null );

        statement.evaluate();
        return uriRef.get();
    }
}
