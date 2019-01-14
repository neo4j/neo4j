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
package org.neo4j.graphdb.impl.notification;


import java.util.Objects;

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
       Status.Statement.CartesianProductWarning,
       "If a part of a query contains multiple disconnected patterns, this will build a " +
       "cartesian product between all those parts. This may produce a large amount of data and slow down" +
       " query processing. " +
       "While occasionally intended, it may often be possible to reformulate the query that avoids the " +
       "use of this cross " +
       "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH"
    ),
    LEGACY_PLANNER(
        SeverityLevel.WARNING,
        Status.Statement.FeatureDeprecationWarning,
        "Using PLANNER for switching between planners has been deprecated, please use CYPHER planner=[rule,cost] instead"
    ),
    DEPRECATED_PLANNER(
        SeverityLevel.WARNING,
        Status.Statement.FeatureDeprecationWarning,
        "The rule planner, which was used to plan this query, is deprecated and will be discontinued soon. " +
                "If you did not explicitly choose the rule planner, you should try to change your query so that the " +
                "rule planner is not used"
    ),
    PLANNER_UNSUPPORTED(
        SeverityLevel.WARNING,
        Status.Statement.PlannerUnsupportedWarning,
        "Using COST planner is unsupported for this query, please use RULE planner instead"
    ),
    RULE_PLANNER_UNAVAILABLE_FALLBACK(
        SeverityLevel.WARNING,
        Status.Statement.PlannerUnavailableWarning,
        "Using RULE planner is unsupported for current CYPHER version, the query has been executed by an older CYPHER " +
        "version"
    ),
    RUNTIME_UNSUPPORTED(
        SeverityLevel.WARNING,
        Status.Statement.RuntimeUnsupportedWarning,
        "Selected runtime is unsupported for this query, please use a different runtime instead or fallback to default."
    ),
    INDEX_HINT_UNFULFILLABLE(
        SeverityLevel.WARNING,
        Status.Schema.IndexNotFound,
        "The hinted index does not exist, please check the schema"
    ),
    JOIN_HINT_UNFULFILLABLE(
        SeverityLevel.WARNING,
        Status.Statement.JoinHintUnfulfillableWarning,
        "The hinted join was not planned. This could happen because no generated plan contained the join key, " +
                "please try using a different join key or restructure your query."
    ),
    JOIN_HINT_UNSUPPORTED(
        SeverityLevel.WARNING,
        Status.Statement.JoinHintUnsupportedWarning,
        "Using RULE planner is unsupported for queries with join hints, please use COST planner instead"
    ),
    LENGTH_ON_NON_PATH(
        SeverityLevel.WARNING,
        Status.Statement.FeatureDeprecationWarning,
        "Using 'length' on anything that is not a path is deprecated, please use 'size' instead"
    ),
    INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY(
        SeverityLevel.WARNING,
        Status.Statement.DynamicPropertyWarning,
        "Using a dynamic property makes it impossible to use an index lookup for this query"
    ),
    BARE_NODE_SYNTAX_DEPRECATED( // This notification is no longer produced by current Cypher compilers
        SeverityLevel.WARNING,   // but it is left here for backwards compatibility.
        Status.Statement.FeatureDeprecationWarning,
        "Use of bare node patterns has been deprecated. Please enclose the identifier in parenthesis."
    ),
    DEPRECATED_FUNCTION(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The query used a deprecated function."
    ),
    DEPRECATED_PROCEDURE(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The query used a deprecated procedure."
    ),
    PROCEDURE_WARNING(
            SeverityLevel.WARNING,
            Status.Procedure.ProcedureWarning,
            "The query used a procedure that generated a warning."
    ),
    DEPRECATED_PROCEDURE_RETURN_FIELD(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The query used a deprecated field from a procedure."
    ),
    DEPRECATED_BINDING_VAR_LENGTH_RELATIONSHIP(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "Binding relationships to a list in a variable length pattern is deprecated."
    ),
    DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The semantics of using colon in the separation of alternative relationship types in conjunction with the " +
            "use of variable binding, inlined property predicates, or variable length will change in a future version."
    ),
    EAGER_LOAD_CSV(
        SeverityLevel.WARNING,
        Status.Statement.EagerOperatorWarning,
        "Using LOAD CSV with a large data set in a query where the execution plan contains the " +
        "Eager operator could potentially consume a lot of memory and is likely to not perform well. " +
        "See the Neo4j Manual entry on the Eager operator for more information and hints on " +
        "how problems could be avoided."
    ),
    LARGE_LABEL_LOAD_CSV(
        SeverityLevel.WARNING,
        Status.Statement.NoApplicableIndexWarning,
        "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely " +
        "not perform well on large data sets. Please consider using a schema index."
        ),
    MISSING_LABEL(
            SeverityLevel.WARNING,
            Status.Statement.UnknownLabelWarning,
            "One of the labels in your query is not available in the database, make sure you didn't " +
            "misspell it or that the label is available when you run this statement in your application"
    ),
    MISSING_REL_TYPE(
            SeverityLevel.WARNING,
            Status.Statement.UnknownRelationshipTypeWarning,
            "One of the relationship types in your query is not available in the database, make sure you didn't " +
            "misspell it or that the label is available when you run this statement in your application"
    ),
    MISSING_PROPERTY_NAME(
            SeverityLevel.WARNING,
            Status.Statement.UnknownPropertyKeyWarning,
            "One of the property names in your query is not available in the database, make sure you didn't " +
            "misspell it or that the label is available when you run this statement in your application"
    ),
    UNBOUNDED_SHORTEST_PATH(
            SeverityLevel.WARNING,
            Status.Statement.UnboundedVariableLengthPatternWarning,
            "Using shortest path with an unbounded pattern will likely result in long execution times. " +
            "It is recommended to use an upper limit to the number of node hops in your pattern."
    ),
    EXHAUSTIVE_SHORTEST_PATH(
            SeverityLevel.WARNING,
            Status.Statement.ExhaustiveShortestPathWarning,
            "Using shortest path with an exhaustive search fallback might cause query slow down since shortest path " +
            "graph algorithms might not work for this use case. It is recommended to introduce a WITH to separate the " +
            "MATCH containing the shortest path from the existential predicates on that path."
    ),
    CREATE_UNIQUE_UNAVAILABLE_FALLBACK(
            SeverityLevel.WARNING,
            Status.Statement.PlannerUnavailableWarning,
        "CREATE UNIQUE is unsupported for current CYPHER version, the query has been executed by an older CYPHER version"
    ),
    START_UNAVAILABLE_FALLBACK(
            SeverityLevel.WARNING,
            Status.Statement.PlannerUnavailableWarning,
            "START is not supported for current CYPHER version, the query has been executed by an older CYPHER version"
    ),
    START_DEPRECATED(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "START has been deprecated and will be removed in a future version." ),
    EXPERIMENTAL_FEATURE(
            SeverityLevel.WARNING,
            Status.Statement.ExperimentalFeature,
            "You are using an experimental feature" ),
    SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY(
            SeverityLevel.INFORMATION,
            Status.Statement.SuboptimalIndexForWildcardQuery,
            "If the performance of this statement using `CONTAINS` doesn't meet your expectations check out the alternative index-providers, see " +
                    "documentation on index configuration." ),
    SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY(
            SeverityLevel.INFORMATION,
            Status.Statement.SuboptimalIndexForWildcardQuery,
            "If the performance of this statement using `ENDS WITH` doesn't meet your expectations check out the alternative index-providers, see " +
                    "documentation on index configuration." );

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

    public final class Notification implements org.neo4j.graphdb.Notification
    {
        private final InputPosition position;
        private final String detailedDescription;

        Notification( InputPosition position, NotificationDetail... details )
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

        @Override
        public String getCode()
        {
            return status.code().serialize();
        }

        @Override
        public String getTitle()
        {
            return status.code().description();
        }

        @Override
        public String getDescription()
        {
            return detailedDescription;
        }

        @Override
        public InputPosition getPosition()
        {
            return position;
        }

        @Override
        public SeverityLevel getSeverity()
        {
            return severity;
        }

        @Override
        public String toString()
        {
            return "Notification{" +
                    "position=" + position +
                    ", detailedDescription='" + detailedDescription + '\'' +
                    '}';
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Notification that = (Notification) o;
            return Objects.equals( position, that.position ) &&
                    Objects.equals( detailedDescription, that.detailedDescription );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( position, detailedDescription );
        }
    }
}
