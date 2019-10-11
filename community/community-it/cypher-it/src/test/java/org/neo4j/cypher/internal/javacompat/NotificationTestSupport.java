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
package org.neo4j.cypher.internal.javacompat;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.impl.notification.NotificationCode;
import org.neo4j.graphdb.impl.notification.NotificationDetail;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

@ImpermanentDbmsExtension
public class NotificationTestSupport
{
    @Inject
    protected GraphDatabaseAPI db;

    void assertNotifications( String query, Matcher<Iterable<Notification>> matchesExpectation )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            try ( Result result = transaction.execute( query ) )
            {
                assertThat( result.getNotifications(), matchesExpectation );
            }
        }
    }

    static Matcher<Notification> notification( String code, Matcher<String> description, Matcher<InputPosition> position, SeverityLevel severity )
    {
        return new TypeSafeMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Notification item )
            {
                return code.equals( item.getCode() ) && description.matches( item.getDescription() ) && position.matches( item.getPosition() ) &&
                        severity.equals( item.getSeverity() );
            }

            @Override
            public void describeTo( Description target )
            {
                target.appendText( "Notification{code=" ).appendValue( code ).appendText( ", description=[" ).appendDescriptionOf( description ).appendText(
                        "], position=[" ).appendDescriptionOf( position ).appendText( "], severity=" ).appendValue( severity ).appendText( "}" );
            }
        };
    }

    static <T> Matcher<Iterable<T>> containsItem( Matcher<T> itemMatcher )
    {
        return new TypeSafeMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Iterable<T> items )
            {
                for ( T item : items )
                {
                    if ( itemMatcher.matches( item ) )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "an iterable containing " ).appendDescriptionOf( itemMatcher );
            }
        };
    }

    static <T> Matcher<Iterable<T>> containsNoItem( Matcher<T> itemMatcher )
    {
        return new TypeSafeMatcher<>()
        {
            @Override
            protected boolean matchesSafely( Iterable<T> items )
            {
                for ( T item : items )
                {
                    if ( itemMatcher.matches( item ) )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "an iterable not containing " ).appendDescriptionOf( itemMatcher );
            }
        };
    }

    void shouldNotifyInStream( String version, String query, InputPosition pos, NotificationCode code )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //when
            try ( Result result = transaction.execute( version + query ) )
            {
                //then
                NotificationCode.Notification notification = code.notification( pos );
                assertThat( Iterables.asList( result.getNotifications() ), Matchers.hasItems( notification ) );
                Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
                assertThat( arguments.get( "version" ), equalTo( version ) );
            }
            transaction.commit();
        }
    }

    void shouldNotifyInStreamWithDetail( String version, String query, InputPosition pos, NotificationCode code,
                                         NotificationDetail detail )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            //when
            try ( Result result = transaction.execute( version + " " + query ) )
            {

                //then
                NotificationCode.Notification notification = code.notification( pos, detail );
                assertThat( Iterables.asList( result.getNotifications() ), Matchers.hasItems( notification ) );
                Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
                assertThat( arguments.get( "version" ), equalTo( version ) );
            }
            transaction.commit();
        }
    }

    void shouldNotNotifyInStream( String version, String query )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // when
            try ( Result result = transaction.execute( version + query ) )
            {

                // then
                assertThat( Iterables.asList( result.getNotifications() ), empty() );
                Map<String,Object> arguments = result.getExecutionPlanDescription().getArguments();
                assertThat( arguments.get( "version" ), equalTo( version ) );
            }
            transaction.commit();
        }
    }

    Matcher<Notification> cartesianProductWarning = notification( "Neo.ClientNotification.Statement.CartesianProductWarning", containsString(
            "If a part of a query contains multiple disconnected patterns, this will build a " +
                    "cartesian product between all those parts. This may produce a large amount of data and slow down" + " query processing. " +
                    "While occasionally intended, it may often be possible to reformulate the query that avoids the " + "use of this cross " +
                    "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH" ), any( InputPosition.class ),
            SeverityLevel.WARNING );

    Matcher<Notification> largeLabelCSVWarning = notification( "Neo.ClientNotification.Statement.NoApplicableIndexWarning", containsString(
            "Using LOAD CSV with a large data set in a query where the execution plan contains the " +
                    "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely " +
                    "not perform well on large data sets. Please consider using a schema index." ), any( InputPosition.class ), SeverityLevel.WARNING );

    Matcher<Notification> eagerOperatorWarning = notification( "Neo.ClientNotification.Statement.EagerOperatorWarning", containsString(
            "Using LOAD CSV with a large data set in a query where the execution plan contains the " +
                    "Eager operator could potentially consume a lot of memory and is likely to not perform well. " +
                    "See the Neo4j Manual entry on the Eager operator for more information and hints on " + "how problems could be avoided." ),
            any( InputPosition.class ), SeverityLevel.WARNING );

    Matcher<Notification> unknownPropertyKeyWarning =
            notification( "Neo.ClientNotification.Statement.UnknownPropertyKeyWarning", containsString( "the missing property name is" ),
                    any( InputPosition.class ), SeverityLevel.WARNING );

    Matcher<Notification> unknownRelationshipWarning =
            notification( "Neo.ClientNotification.Statement.UnknownRelationshipTypeWarning", containsString( "the missing relationship type is" ),
                    any( InputPosition.class ), SeverityLevel.WARNING );

    Matcher<Notification> unknownLabelWarning =
            notification( "Neo.ClientNotification.Statement.UnknownLabelWarning", containsString( "the missing label name is" ), any( InputPosition.class ),
                    SeverityLevel.WARNING );

    Matcher<Notification> dynamicPropertyWarning = notification( "Neo.ClientNotification.Statement.DynamicPropertyWarning",
            containsString( "Using a dynamic property makes it impossible to use an index lookup for this query" ), any( InputPosition.class ),
            SeverityLevel.WARNING );
}
