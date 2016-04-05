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
package org.neo4j.bolt.v1.runtime.integration;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.kernel.api.exceptions.Status;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.failedWith;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.recorded;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.success;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.successButRequiresPasswordChange;
import static org.neo4j.helpers.collection.MapUtil.map;

public class SessionAuthIT
{
    @Rule
    public TestSessions env = new TestSessions().withAuthEnabled( true );

    @Test
    public void shouldGiveCredentialsExpiredStatusOnExpiredCredentials() throws Throwable
    {
        // given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
        Session session = env.newSession( "test" );
        RecordingCallback recorder = new RecordingCallback();

        // when
        session.init( "TestClient/1.0.0", map(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j" ), null, recorder );
        session.run( "CREATE ()", map(), null, recorder );

        // then
        assertThat( recorder, recorded(
                successButRequiresPasswordChange(),
                failedWith( Status.Security.CredentialsExpired ) ) );
    }

    @Test
    public void shouldBeAbleToActOnSessionWhenUpdatingCredentials() throws Throwable
    {
        // given it is important for client applications to programmatically
        // identify expired credentials as the cause of not being authenticated
        Session session = env.newSession( "test" );
        RecordingCallback recorder = new RecordingCallback();

        // when
        session.init( "TestClient/1.0.0", map(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j",
                "new_credentials", "secret"
                ), null, recorder );
        session.run( "CREATE ()", map(), null, recorder );

        // then
        assertThat( recorder, recorded( success(), success() ));
    }
}
