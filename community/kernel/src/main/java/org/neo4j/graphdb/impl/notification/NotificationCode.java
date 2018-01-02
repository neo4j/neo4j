/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.graphdb.impl.notification;


import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Notification codes are status codes identifying the type of notification.
 */
public enum NotificationCode
{
    CARTESIAN_PRODUCT(
       SeverityLevel.WARNING,
       Status.Statement.CartesianProduct,
       "If a part of a query contains multiple disconnected patterns, this will build a " +
       "cartesian product between all those parts. This may produce a large amount of data and slow down" +
       " query processing. " +
       "While occasionally intended, it may often be possible to reformulate the query that avoids the " +
       "use of this cross " +
       "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH"
    ),
    LEGACY_PLANNER(
        SeverityLevel.WARNING,
        Status.Statement.DeprecationWarning,
        "Using PLANNER for switching between planners has been deprecated, please use CYPHER planner=[rule,cost] instead"
    ),
    PLANNER_UNSUPPORTED(
        SeverityLevel.WARNING,
        Status.Statement.PlannerUnsupportedWarning,
        "Using COST planner is unsupported for this query, please use RULE planner instead"
    ),
    RUNTIME_UNSUPPORTED(
        SeverityLevel.WARNING,
        Status.Statement.RuntimeUnsupportedWarning,
        "Using COMPILED runtime is unsupported for this query, please use interpreted runtime instead"
    ),
    INDEX_HINT_UNFULFILLABLE(
        SeverityLevel.WARNING,
        Status.Schema.NoSuchIndex,
        "The hinted index does not exist, please check the schema"
    ),
    JOIN_HINT_UNFULFILLABLE(
        SeverityLevel.WARNING,
        Status.Statement.JoinHintUnfulfillableWarning,
        "The hinted join was not planned. This could happen because no generated plan contained the join key, please try using a different join key or restructure your query."
    ),
    JOIN_HINT_UNSUPPORTED(
        SeverityLevel.WARNING,
        Status.Statement.JoinHintUnsupportedWarning,
        "Using RULE planner is unsupported for queries with join hints, please use COST planner instead"
    ),
    LENGTH_ON_NON_PATH(
        SeverityLevel.WARNING,
        Status.Statement.DeprecationWarning,
        "Using 'length' on anything that is not a path is deprecated, please use 'size' instead"
    ),
    INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY(
        SeverityLevel.WARNING,
        Status.Statement.DynamicPropertyWarning,
        "Using a dynamic property makes it impossible to use an index lookup for this query"
    ),
    BARE_NODE_SYNTAX_DEPRECATED(
        SeverityLevel.WARNING,
        Status.Statement.DeprecationWarning,
        "Use of bare node patterns has been deprecated. Please enclose the identifier in parenthesis."
    ),
    EAGER_LOAD_CSV(
        SeverityLevel.WARNING,
        Status.Statement.EagerWarning,
        "Using LOAD CSV with a large data set in a query where the execution plan contains the " +
        "Eager operator could potentially consume a lot of memory and is likely to not perform well. " +
        "See the Neo4j Manual entry on the Eager operator for more information and hints on " +
        "how problems could be avoided."
    ),
    LARGE_LABEL_LOAD_CSV(
        SeverityLevel.WARNING,
        Status.Statement.IndexMissingWarning,
        "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely " +
        "not perform well on large data sets. Please consider using a schema index."
        ),
    MISSING_LABEL(
            SeverityLevel.WARNING,
            Status.Statement.LabelMissingWarning,
            "One of the labels in your query is not available in the database, make sure you didn't " +
            "misspell it or that the label is available when you run this statement in your application"
    ),
    MISSING_REL_TYPE(
            SeverityLevel.WARNING,
            Status.Statement.RelTypeMissingWarning,
            "One of the relationship types in your query is not available in the database, make sure you didn't " +
            "misspell it or that the label is available when you run this statement in your application"
    ),
    MISSING_PROPERTY_NAME(
            SeverityLevel.WARNING,
            Status.Statement.PropertyNameMissingWarning,
            "One of the property names in your query is not available in the database, make sure you didn't " +
            "misspell it or that the label is available when you run this statement in your application"
    ),
    UNBOUNDED_SHORTEST_PATH(
            SeverityLevel.WARNING,
            Status.Statement.UnboundedPatternWarning,
            "Using shortest path with an unbounded pattern will likely result in long execution times. " +
            "It is recommended to use an upper limit to the number of node hops in your pattern."
    )
    ;

    private final Status status;
    private final String description;
    private final SeverityLevel severity;

    NotificationCode( SeverityLevel severity, Status status, String description )
    {
        this.severity = severity;
        this.status = status;
        this.description = description;
    }

    // TODO: Move construction of Notifications to a factory with explicit methods per type of notification
    public Notification notification( InputPosition position, NotificationDetail... details )
    {
        return new Notification( position, details );
    }

    private final class Notification implements org.neo4j.graphdb.Notification
    {
        private final InputPosition position;
        private final String detailedDescription;

        public Notification( InputPosition position, NotificationDetail... details )
        {
            this.position = position;

            if ( details.length == 0 )
            {
                this.detailedDescription = description;
            }
            else
            {
                StringBuilder builder = new StringBuilder( description.length() );
                builder.append( description );
                builder.append( ' ' );
                builder.append( '(' );
                String comma = "";
                for ( NotificationDetail detail : details )
                {
                    builder.append( comma );
                    builder.append( detail );
                    comma = ", ";
                }
                builder.append( ')' );

                this.detailedDescription = builder.toString();
            }
        }

        public String getCode()
        {
            return status.code().serialize();
        }

        public String getTitle()
        {
            return status.code().description();
        }

        public String getDescription()
        {
            return detailedDescription;
        }

        public InputPosition getPosition()
        {
            return position;
        }

        public SeverityLevel getSeverity()
        {
            return severity;
        }

        @Override
        public String toString() {
            return "Notification{" +
                    "position=" + position +
                    ", detailedDescription='" + detailedDescription + '\'' +
                    '}';
        }
    }
}
