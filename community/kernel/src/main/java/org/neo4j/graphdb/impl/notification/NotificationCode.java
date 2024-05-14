/*
 * Copyright (c) "Neo4j"
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
    DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The semantics of using colon in the separation of alternative relationship types in conjunction with the " +
            "use of variable binding, inlined property predicates, or variable length will change in a future version."
    ),
    DEPRECATED_PARAMETER_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The parameter syntax `{param}` is deprecated, please use `$param` instead"
    ),
    DEPRECATED_CREATE_INDEX_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The create index syntax `CREATE INDEX ON :Label(property)` is deprecated, please use `CREATE INDEX FOR (n:Label) ON (n.property)` instead"
    ),
    DEPRECATED_BTREE_INDEX_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "B-tree indexes are deprecated, partially replaced by text indexes and will be fully replaced later on. " +
                    "For now, b-tree indexes are still the correct alternative to use."
    ),
    DEPRECATED_DROP_INDEX_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The drop index syntax `DROP INDEX ON :Label(property)` is deprecated, please use `DROP INDEX index_name` instead"
    ),
    DEPRECATED_DROP_CONSTRAINT_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The drop constraint by schema syntax `DROP CONSTRAINT ON ...` is deprecated, please use `DROP CONSTRAINT constraint_name` instead"
    ),
    DEPRECATED_CREATE_PROPERTY_EXISTENCE_CONSTRAINT_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The create property existence constraint syntax `CREATE CONSTRAINT ON ... ASSERT exists(variable.property)` is deprecated, " +
                    "please use `CREATE CONSTRAINT FOR ... REQUIRE (variable.property) IS NOT NULL` instead"
    ),
    DEPRECATED_CREATE_CONSTRAINT_ON_ASSERT_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The create constraint syntax `CREATE CONSTRAINT ON ... ASSERT ...` is deprecated, " +
                    "please use `CREATE CONSTRAINT FOR ... REQUIRE ...` instead"
    ),
    DEPRECATED_SHOW_SCHEMA_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The `BRIEF` and `VERBOSE` keywords for `SHOW INDEXES` and `SHOW CONSTRAINTS` are deprecated, " +
                    "please omit `BRIEF` and use `YIELD *` instead of `VERBOSE`"
    ),
    DEPRECATED_SHOW_EXISTENCE_CONSTRAINT_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The `EXISTS` keyword for `SHOW CONSTRAINTS` are deprecated, please use `EXIST` instead"
    ),
    DEPRECATED_PROPERTY_EXISTENCE_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The property existence syntax `... exists(variable.property)` is deprecated, please use `variable.property IS NOT NULL` instead"
    ),
    DEPRECATED_DEFAULT_DATABASE_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The `ON DEFAULT DATABASE` syntax is deprecated, use `ON HOME DATABASE` instead"
    ),
    DEPRECATED_DEFAULT_GRAPH_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The `ON DEFAULT GRAPH` syntax is deprecated, use `ON HOME GRAPH` instead"
    ),
    DEPRECATED_CATALOG_KEYWORD_FOR_ADMIN_COMMAND_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The optional `CATALOG` prefix for administration commands has been deprecated and should be omitted."
    ),
    DEPRECATED_PERIODIC_COMMIT(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The usage of the PERIODIC COMMIT hint has been deprecated. Please use a transactional subquery (e.g. `CALL { ... } IN TRANSACTIONS`) instead."
    ),
    DEPRECATED_OCTAL_LITERAL_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The octal integer literal syntax `0123` is deprecated, please use `0o123` instead"
    ),
    DEPRECATED_HEX_LITERAL_SYNTAX(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The hex integer literal syntax `0X123` is deprecated, please use `0x123` instead"
    ),
    DEPRECATED_USE_OF_PATTERN_EXPRESSION(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "A pattern expression should only be used in order to test the existence of a pattern. " +
            "It should therefore only be used in contexts that evaluate to a boolean, e.g. inside the function exists() or in a WHERE-clause. " +
            "All other uses are deprecated and should be replaced by a pattern comprehension."
    ),
    DEPRECATED_COERCION_OF_LIST_TO_BOOLEAN(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "Coercion of list to boolean is deprecated. Please consider using `NOT isEmpty(...)` instead."
    ),
    DEPRECATED_SELF_REFERENCE_TO_VARIABLE_IN_CREATE_PATTERN(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "Referencing a node or relationship variable that is created in the same CREATE clause is deprecated. " +
            "The behaviour of using this syntax is undefined and should be avoided. Please consider only referencing variables created in earlier clauses."
    ),
    DEPRECATED_POINTS_COMPARE(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "The behavior when comparing spatial points with '<', '<=', '>', and '>=` will change. Please use the 'point.withinBBox' or 'point.distance'" +
            " for seeking spatial points within a specific range."
    ),
    DEPRECATED_AMBIGUOUS_GROUPING_NOTIFICATION(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "Aggregation column contains implicit grouping expressions. " +
            "Aggregation expressions with implicit grouping keys are deprecated and will be removed in a future version. " +
            "For example, in 'RETURN n.a, n.a + n.b + count(*)' the aggregation expression 'n.a + n.b + count(*)' includes the implicit grouping key 'n.b', " +
            "and this expression is now deprecated. " +
            "It may be possible to rewrite the query by extracting these grouping/aggregation expressions into a preceding WITH clause."
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
    EXPERIMENTAL_FEATURE(
            SeverityLevel.WARNING,
            Status.Statement.ExperimentalFeature,
            "You are using an experimental feature" ),
    MISSING_PARAMETERS_FOR_EXPLAIN(
            SeverityLevel.WARNING,
            Status.Statement.ParameterMissing,
            "Did not supply query with enough parameters. The produced query plan will not be cached and is not executable without EXPLAIN." ),
    SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY(
            SeverityLevel.INFORMATION,
            Status.Statement.SuboptimalIndexForWildcardQuery,
            "If the performance of this statement using `CONTAINS` doesn't meet your expectations check out the alternative index-providers, see " +
                    "documentation on index configuration." ),
    SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY(
            SeverityLevel.INFORMATION,
            Status.Statement.SuboptimalIndexForWildcardQuery,
            "If the performance of this statement using `ENDS WITH` doesn't meet your expectations check out the alternative index-providers, see " +
                    "documentation on index configuration." ),
    CODE_GENERATION_FAILED(
            SeverityLevel.WARNING,
            Status.Statement.CodeGenerationFailed,
            "The database was unable to generate code for the query. A stacktrace can be found in the debug.log." ),
    REPEATED_REL_IN_PATTERN_EXPRESSION(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "You are using the same relationship variable for multiple patterns in a pattern expression/comprehension. " +
            "This feature is deprecated and will be removed in a future version, " +
            "because it does not follow Cyphers pattern matching relationship uniqueness rule. " +
            "It can lead to the optimizer choosing bad plans for that pattern expression/comprehension. " +
            "Please rewrite your query, using the start node and/or end node of the relationship in the pattern expression/comprehension instead."
    ),
    SUBQUERY_VARIABLE_SHADOWING(
            SeverityLevel.WARNING,
            Status.Statement.SubqueryVariableShadowingWarning,
            "Variable in subquery is shadowing a variable with the same name from the outer scope. " +
            "If you want to use that variable instead, it must be imported into the subquery using importing WITH clause."
    ),
    MISSING_ALIAS(
            SeverityLevel.WARNING,
            Status.Statement.MissingAlias,
            "There is no alias for one or more complex returned items in a RETURN clause in a CALL subquery. " +
            "All returned items except variables, e.g. 'RETURN n', and map projections, e.g. 'RETURN n { .prop, .prop2 }' " +
            "should be aliased explicitly using 'AS'. The support for such unaliased returned items will be removed in a future version."
    ),
    DEPRECATED_CASE_EXPRESSION(
            SeverityLevel.WARNING,
            Status.Statement.FeatureDeprecationWarning,
            "Using null as an expression to be compared against in a CASE expression is deprecated and from 5.0 will no longer match on anything. " +
                    "Try using a generic CASE with `IS NULL` instead. For example 'CASE WHEN n.prop IS NULL THEN true ELSE false END'."
    );

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

        public Status getStatus()
        {
            return status;
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
