/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.transport.http;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.FakeClock;
import org.neo4j.ndp.runtime.Sessions;
import org.neo4j.ndp.runtime.Session;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SessionRegistryTest
{
    private final Sessions environment = mock( Sessions.class );
    private final Session session = mock( Session.class );

    @Test
    public void shouldBeAbleToCreateAndDestroySession() throws Throwable
    {
        // Given
        when( session.key() ).thenReturn( "1234" );
        when( environment.newSession() ).thenReturn( session );
        org.neo4j.ndp.transport.http.SessionRegistry registry = new org.neo4j.ndp.transport.http.SessionRegistry( environment );

        // When
        String sessionKey = registry.create();

        // Then
        assertThat( sessionKey, notNullValue() );
        assertThat( sessionKey.length(), greaterThan( 0 ) );
        assertThat( registry.acquire( sessionKey ).success(), equalTo( true ) );

        // When
        registry.destroy( sessionKey );

        // Then
        assertThat( sessionKey, notNullValue() );
        assertThat( sessionKey.length(), greaterThan( 0 ) );
        assertThat( registry.acquire( sessionKey ).success(), equalTo( false ) );

    }

    @Test
    public void shouldNotBeAbleToAcquireTwice() throws Throwable
    {
        // Given
        when( session.key() ).thenReturn( "1234" );
        when( environment.newSession() ).thenReturn( session );
        org.neo4j.ndp.transport.http.SessionRegistry registry = new org.neo4j.ndp.transport.http.SessionRegistry( environment );

        String sessionKey = registry.create();
        registry.acquire( sessionKey );

        // When
        org.neo4j.ndp.transport.http.SessionAcquisition acquisition = registry.acquire( sessionKey );

        // Then
        assertThat( acquisition.success(), equalTo( false ) );
        assertThat( acquisition.sessionExists(), equalTo( true ) );
        assertThat( acquisition.session(), nullValue() );
    }

    @Test
    public void shouldBeAbleToAcquireReleaseAndAcquireAgain() throws Throwable
    {
        // Given
        when( session.key() ).thenReturn( "1234" );
        when( environment.newSession() ).thenReturn( session );
        org.neo4j.ndp.transport.http.SessionRegistry registry = new org.neo4j.ndp.transport.http.SessionRegistry( environment );

        String sessionKey = registry.create();
        registry.acquire( sessionKey );
        registry.release( sessionKey );

        // When
        org.neo4j.ndp.transport.http.SessionAcquisition acquisition = registry.acquire( sessionKey );

        // Then
        assertThat( acquisition.success(), equalTo( true ) );
        assertThat( acquisition.sessionExists(), equalTo( true ) );
        assertThat( acquisition.session(), equalTo( session ) );
    }

    @Test
    public void shouldEvictIdleSessions() throws Throwable
    {
        // Given
        when( session.key() ).thenReturn( "1234" );
        when( environment.newSession() ).thenReturn( session );
        FakeClock clock = new FakeClock();
        org.neo4j.ndp.transport.http.SessionRegistry registry = new SessionRegistry( environment, clock );

        String sessionKey = registry.create();

        clock.forward( 61, TimeUnit.SECONDS );

        // When
        registry.destroyIdleSessions( 60, TimeUnit.SECONDS );

        // Then the session should've gotten closed
        verify(session).close();

        // And it should no longer be available
        assertFalse( registry.acquire( sessionKey ).sessionExists() );
    }
}
