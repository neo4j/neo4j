/*
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
package org.neo4j.bolt.runtime.integration;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import org.neo4j.bolt.runtime.Session;
import org.neo4j.bolt.runtime.StatementMetadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.runtime.integration.SessionMatchers.success;

public class TransactionIT
{
    private static final Map<String, Object> EMPTY_PARAMS = Collections.emptyMap();

    @Rule
    public TestSessions env = new TestSessions();

    @Test
    public void shouldHandleBeginCommit() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata, ?> responses = new RecordingCallback<>();
        Session session = env.newSession();
        session.initialize( "TestClient", null, null );

        // When
        session.run( "BEGIN", EMPTY_PARAMS, null, responses );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        session.run( "CREATE (n:InTx)", EMPTY_PARAMS, null, responses );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        session.run( "COMMIT", EMPTY_PARAMS, null, responses );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        // Then
        assertThat( responses.next(), success() );
        assertThat( responses.next(), success() );
        assertThat( responses.next(), success() );
    }

    @Test
    public void shouldHandleBeginRollback() throws Throwable
    {
        // Given
        RecordingCallback<StatementMetadata, ?> responses = new RecordingCallback<>();
        Session session = env.newSession();
        session.initialize( "TestClient", null, null );

        // When
        session.run( "BEGIN", EMPTY_PARAMS, null, responses );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        session.run( "CREATE (n:InTx)", EMPTY_PARAMS, null, responses );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        session.run( "ROLLBACK", EMPTY_PARAMS, null, responses );
        session.discardAll( null, Session.Callbacks.<Void,Object>noop() );

        // Then
        assertThat( responses.next(), success() );
        assertThat( responses.next(), success() );
        assertThat( responses.next(), success() );
    }
}
