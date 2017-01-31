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
package org.neo4j.cypher.internal.javacompat;

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.impl.notification.NotificationCode.RULE_PLANNER_UNAVAILABLE_FALLBACK;

public class NotificationAcceptanceTest
{
    @Rule
    public final ImpermanentDatabaseRule rule = new ImpermanentDatabaseRule();

    @Test
    public void shouldNotifyWhenUsingCypher3_1ForTheRulePlannerWhenCypherVersionIsTheDefault() throws Exception
    {
        // when
        Result result = db().execute( "CYPHER planner=rule RETURN 1" );
        InputPosition position = new InputPosition( 20, 1, 21 );

        // then
        assertThat( result.getNotifications(), contains( RULE_PLANNER_UNAVAILABLE_FALLBACK.notification( position ) ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.1" ) );
        assertThat( arguments.get( "planner" ), equalTo( "RULE" ) );
        result.close();
    }

    @Test
    public void shouldNotifyWhenUsingCypher3_1ForTheRulePlannerWhenCypherVersionIs3_2() throws Exception
    {
        // when
        Result result = db().execute( "CYPHER 3.2 planner=rule RETURN 1" );
        InputPosition position = new InputPosition( 24, 1, 25 );

        // then
        assertThat( result.getNotifications(), contains( RULE_PLANNER_UNAVAILABLE_FALLBACK.notification( position ) ) );
        Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
        assertThat( arguments.get( "version" ), equalTo( "CYPHER 3.1" ) );
        assertThat( arguments.get( "planner" ), equalTo( "RULE" ) );
        result.close();
    }

    @Test
    public void shouldNotNotifyWhenUsingTheRulePlannerWhenCypherVersionIsNot3_2() throws Exception
    {
        Stream.of( "CYPHER 3.1", "CYPHER 2.3" ).forEach( version ->
        {
            // when
            Result result = db().execute( version + " planner=rule RETURN 1" );

            // then
            assertThat( Iterables.asList( result.getNotifications() ), empty() );
            Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
            assertThat( arguments.get( "version" ), equalTo( version ) );
            assertThat( arguments.get( "planner" ), equalTo( "RULE" ) );
            result.close();
        });
    }

    private GraphDatabaseAPI db()
    {
        return rule.getGraphDatabaseAPI();
    }
}
