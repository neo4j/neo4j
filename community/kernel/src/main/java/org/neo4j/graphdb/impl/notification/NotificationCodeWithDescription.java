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

import org.neo4j.graphdb.InputPosition;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;

/**
 * This bundles a specific description with a (potentially) more generic NotificationCode.
 *
 * If changing or adding a notification, please make sure you follow the guidelines here: <a href="https://github.com/neo4j/docs-status-codes/blob/dev/README.adoc">NOTIFICATION GUIDELINES</a>
 */
public enum NotificationCodeWithDescription {
    CARTESIAN_PRODUCT(
            Status.Statement.CartesianProduct,
            "If a part of a query contains multiple disconnected patterns, this will build a "
                    + "cartesian product between all those parts. This may produce a large amount of data and slow down"
                    + " query processing. "
                    + "While occasionally intended, it may often be possible to reformulate the query that avoids the "
                    + "use of this cross "
                    + "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH (%s)"),
    RUNTIME_UNSUPPORTED(
            Status.Statement.RuntimeUnsupportedWarning,
            "Selected runtime is unsupported for this query, please use a different runtime instead or fallback to default. (%s)"),
    INDEX_HINT_UNFULFILLABLE(
            Status.Schema.HintedIndexNotFound, "The hinted index does not exist, please check the schema (%s)"),
    JOIN_HINT_UNFULFILLABLE(
            Status.Statement.JoinHintUnfulfillableWarning,
            "The hinted join was not planned. This could happen because no generated plan contained the join key, "
                    + "please try using a different join key or restructure your query. (%s)"),
    INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY(
            Status.Statement.DynamicProperty,
            "Using a dynamic property makes it impossible to use an index lookup for this query (%s)"),
    DEPRECATED_FUNCTION(Status.Statement.FeatureDeprecationWarning, "The query used a deprecated function%s"),
    DEPRECATED_PROCEDURE(Status.Statement.FeatureDeprecationWarning, "The query used a deprecated procedure%s"),

    DEPRECATED_RUNTIME_OPTION(
            Status.Statement.FeatureDeprecationWarning, "The query used a deprecated runtime option. (%s)"),
    PROCEDURE_WARNING(Status.Procedure.ProcedureWarning, "The query used a procedure that generated a warning. (%s)"),
    DEPRECATED_PROCEDURE_RETURN_FIELD(
            Status.Statement.FeatureDeprecationWarning, "The query used a deprecated field from a procedure. (%s)"),
    DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR(
            Status.Statement.FeatureDeprecationWarning,
            "The semantics of using colon in the separation of alternative relationship types will change in a future version. (%s)"),
    DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE(
            Status.Statement.FeatureDeprecationWarning,
            "The use of nodes or relationships for setting properties is deprecated and will be removed in a future version. "
                    + "Please use properties() instead."),

    DEPRECATED_PROPERTY_REFERENCE_IN_CREATE(
            Status.Statement.FeatureDeprecationWarning,
            // referencing that entity in a property definition in the same create clause
            "Creating an entity (%s) and referencing that entity in a property definition in the same CREATE is deprecated."),

    DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP(
            Status.Statement.FeatureDeprecationWarning,
            "The use of shortestPath and allShortestPaths with fixed length relationships is deprecated and will be removed in a future version. "
                    + "Please use a path with a length of 1 [r*1..1] instead or a Match with a limit."),
    DEPRECATED_TEXT_INDEX_PROVIDER(
            Status.Statement.FeatureDeprecationWarning,
            "The `" + TextIndexProvider.DESCRIPTOR.name()
                    + "` provider for text indexes is deprecated and will be removed in a future version. "
                    + "Please use `" + TrigramIndexProvider.DESCRIPTOR.name() + "` instead."),
    EAGER_LOAD_CSV(
            Status.Statement.EagerOperator,
            "Using LOAD CSV with a large data set in a query where the execution plan contains the "
                    + "Eager operator could potentially consume a lot of memory and is likely to not perform well. "
                    + "See the Neo4j Manual entry on the Eager operator for more information and hints on "
                    + "how problems could be avoided."),
    DEPRECATED_FORMAT(Status.Request.DeprecatedFormat, "The requested format has been deprecated. (%s)"),
    LARGE_LABEL_LOAD_CSV(
            Status.Statement.NoApplicableIndex,
            "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely "
                    + "not perform well on large data sets. Please consider using a schema index."),
    MISSING_LABEL(
            Status.Statement.UnknownLabelWarning,
            "One of the labels in your query is not available in the database, make sure you didn't "
                    + "misspell it or that the label is available when you run this statement in your application (%s)"),
    MISSING_REL_TYPE(
            Status.Statement.UnknownRelationshipTypeWarning,
            "One of the relationship types in your query is not available in the database, make sure you didn't "
                    + "misspell it or that the label is available when you run this statement in your application (%s)"),
    MISSING_PROPERTY_NAME(
            Status.Statement.UnknownPropertyKeyWarning,
            "One of the property names in your query is not available in the database, make sure you didn't "
                    + "misspell it or that the label is available when you run this statement in your application (%s)"),
    UNBOUNDED_SHORTEST_PATH(
            Status.Statement.UnboundedVariableLengthPattern,
            "Using shortest path with an unbounded pattern will likely result in long execution times. "
                    + "It is recommended to use an upper limit to the number of node hops in your pattern."),
    EXHAUSTIVE_SHORTEST_PATH(
            Status.Statement.ExhaustiveShortestPath,
            "Using shortest path with an exhaustive search fallback might cause query slow down since shortest path "
                    + "graph algorithms might not work for this use case. It is recommended to introduce a WITH to separate the "
                    + "MATCH containing the shortest path from the existential predicates on that path."),
    RUNTIME_EXPERIMENTAL(Status.Statement.RuntimeExperimental, "You are using an experimental feature (%s)"),
    MISSING_PARAMETERS_FOR_EXPLAIN(
            Status.Statement.ParameterNotProvided,
            "Did not supply query with enough parameters. The produced query plan will not be cached and is not executable without EXPLAIN. (%s)"),
    CODE_GENERATION_FAILED(
            Status.Statement.CodeGenerationFailed,
            "The database was unable to generate code for the query. A stacktrace can be found in the debug.log. (%s)"),
    SUBQUERY_VARIABLE_SHADOWING(
            Status.Statement.SubqueryVariableShadowing,
            "Variable in subquery is shadowing a variable with the same name from the outer scope. "
                    + "If you want to use that variable instead, it must be imported into the subquery using importing WITH clause. (%s)"),
    UNION_RETURN_ORDER(
            Status.Statement.FeatureDeprecationWarning,
            "All subqueries in a UNION [ALL] should have the same ordering for the return columns. "
                    + "Using differently ordered return items in a UNION [ALL] clause is deprecated and will be removed in a future version."),
    HOME_DATABASE_NOT_PRESENT(
            Status.Database.HomeDatabaseNotFound,
            "The home database provided does not currently exist in the DBMS. This command will not take effect until this database is created. (%s)"),
    DEPRECATED_DATABASE_NAME(
            Status.Statement.FeatureDeprecationWarning,
            "Databases and aliases with unescaped `.` are deprecated unless to indicate that they belong to a composite database. "
                    + "Names containing `.` should be escaped. (%s)"),
    UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION(
            Status.Statement.UnsatisfiableRelationshipTypeExpression,
            "Relationship type expression cannot possibly be satisfied. (%s)"),
    REPEATED_RELATIONSHIP_REFERENCE(
            Status.Statement.RepeatedRelationshipReference,
            "A relationship is referenced more than once in the query, which leads to no results because"
                    + " relationships must not occur more than once in each result. (%s)"),
    REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE(
            Status.Statement.RepeatedRelationshipReference,
            "A variable-length relationship variable is bound more than once, which leads to no results because"
                    + " relationships must not occur more than once in each result. (%s)"),
    DEPRECATED_CONNECT_COMPONENTS_PLANNER_PRE_PARSER_OPTION(
            Status.Statement.FeatureDeprecationWarning,
            "The Cypher query option `connectComponentsPlanner` is deprecated and will be removed without a replacement. "
                    + "The product's default behavior of using a cost-based IDP search algorithm when combining sub-plans will be kept. "
                    + "For more information, see Cypher Manual -> Cypher planner."),
    COMMAND_HAS_NO_EFFECT(
            Status.Security.CommandHasNoEffect, "%s See Status Codes documentation for more information."),
    IMPOSSIBLE_REVOKE_COMMAND(
            Status.Security.ImpossibleRevokeCommand,
            "%s Make sure nothing is misspelled. This notification will become an error in a future major version. "
                    + "See Status Codes documentation for more information.");

    private final Status status;
    private final String description;
    private final String message;

    NotificationCodeWithDescription(Status status, String description) {
        this.status = status;
        this.description = description;
        this.message = null;
    }

    NotificationCodeWithDescription(Status status, String description, String message) {
        this.status = status;
        this.description = description;
        this.message = message;
    }

    public Status getStatus() {
        return status;
    }

    public String getDescription() {
        return description;
    }

    public String getMessage() {
        return message;
    }

    public static NotificationImplementation cartesianProduct(InputPosition position, String param) {
        return CARTESIAN_PRODUCT.notification(position, param);
    }

    public static NotificationImplementation runtimeUnsupported(InputPosition position, String param) {
        return RUNTIME_UNSUPPORTED.notification(position, param);
    }

    public static NotificationImplementation indexHintUnfulfillable(InputPosition position, String param) {
        return INDEX_HINT_UNFULFILLABLE.notification(position, param);
    }

    public static NotificationImplementation joinHintUnfulfillable(InputPosition position, String param) {
        return JOIN_HINT_UNFULFILLABLE.notification(position, param);
    }

    public static NotificationImplementation indexLookupForDynamicProperty(InputPosition position, String param) {
        return INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(position, param);
    }

    public static NotificationImplementation deprecatedFunction(InputPosition position, String param) {
        return DEPRECATED_FUNCTION.notification(position, param);
    }

    public static NotificationImplementation deprecatedProcedure(InputPosition position, String param) {
        return DEPRECATED_PROCEDURE.notification(position, param);
    }

    public static NotificationImplementation deprecatedRuntimeOption(InputPosition position, String param) {
        return DEPRECATED_RUNTIME_OPTION.notification(position, param);
    }

    public static NotificationImplementation procedureWarning(InputPosition position, String param) {
        return PROCEDURE_WARNING.notification(position, param);
    }

    public static NotificationImplementation deprecatedProcedureReturnField(InputPosition position, String param) {
        return DEPRECATED_PROCEDURE_RETURN_FIELD.notification(position, param);
    }

    public static NotificationImplementation deprecatedRelationshipTypeSeparator(InputPosition position, String param) {
        return DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(position, param);
    }

    public static NotificationImplementation deprecatedNodeOrRelationshipOnRhsSetClause(InputPosition position) {
        return DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE.notification(position);
    }

    public static NotificationImplementation deprecatedPropertyReferenceInCreate(InputPosition position, String param) {
        return DEPRECATED_PROPERTY_REFERENCE_IN_CREATE.notification(position, param);
    }

    public static NotificationImplementation deprecatedShortestPathWithFixedLengthRelationship(InputPosition position) {
        return DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP.notification(position);
    }

    public static NotificationImplementation deprecatedTextIndexProvider(InputPosition position) {
        return DEPRECATED_TEXT_INDEX_PROVIDER.notification(position);
    }

    public static NotificationImplementation eagerLoadCsv(InputPosition position) {
        return EAGER_LOAD_CSV.notification(position);
    }

    public static NotificationImplementation deprecatedFormat(InputPosition position, String param) {
        return DEPRECATED_FORMAT.notification(position, param);
    }

    public static NotificationImplementation largeLabelLoadCsv(InputPosition position) {
        return LARGE_LABEL_LOAD_CSV.notification(position);
    }

    public static NotificationImplementation missingLabel(InputPosition position, String param) {
        return MISSING_LABEL.notification(position, param);
    }

    public static NotificationImplementation missingRelType(InputPosition position, String param) {
        return MISSING_REL_TYPE.notification(position, param);
    }

    public static NotificationImplementation missingPropertyName(InputPosition position, String param) {
        return MISSING_PROPERTY_NAME.notification(position, param);
    }

    public static NotificationImplementation unboundedShortestPath(InputPosition position) {
        return UNBOUNDED_SHORTEST_PATH.notification(position);
    }

    public static NotificationImplementation exhaustiveShortestPath(InputPosition position) {
        return EXHAUSTIVE_SHORTEST_PATH.notification(position);
    }

    public static NotificationImplementation runtimeExperimental(InputPosition position, String param) {
        return RUNTIME_EXPERIMENTAL.notification(position, param);
    }

    public static NotificationImplementation missingParameterForExplain(InputPosition position, String param) {
        return MISSING_PARAMETERS_FOR_EXPLAIN.notification(position, param);
    }

    public static NotificationImplementation codeGenerationFailed(InputPosition position, String param) {
        return CODE_GENERATION_FAILED.notification(position, param);
    }

    public static NotificationImplementation subqueryVariableShadowing(InputPosition position, String param) {
        return SUBQUERY_VARIABLE_SHADOWING.notification(position, param);
    }

    public static NotificationImplementation unionReturnOrder(InputPosition position) {
        return UNION_RETURN_ORDER.notification(position);
    }

    public static NotificationImplementation homeDatabaseNotPresent(InputPosition position, String param) {
        return HOME_DATABASE_NOT_PRESENT.notification(position, param);
    }

    public static NotificationImplementation deprecatedDatabaseName(InputPosition position, String param) {
        return DEPRECATED_DATABASE_NAME.notification(position, param);
    }

    public static NotificationImplementation unsatisfiableRelationshipTypeExpression(
            InputPosition position, String param) {
        return UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION.notification(position, param);
    }

    public static NotificationImplementation repeatedRelationshipReference(InputPosition position, String param) {
        return REPEATED_RELATIONSHIP_REFERENCE.notification(position, param);
    }

    public static NotificationImplementation repeatedVarLengthRelationshipReference(
            InputPosition position, String param) {
        return REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE.notification(position, param);
    }

    public static NotificationImplementation deprecatedConnectComponentsPlannerPreParserOption(InputPosition position) {
        return DEPRECATED_CONNECT_COMPONENTS_PLANNER_PRE_PARSER_OPTION.notification(position);
    }

    public static NotificationImplementation commandHasNoEffect(
            InputPosition position, String titleParam, String descriptionParam) {
        return COMMAND_HAS_NO_EFFECT.notificationWithTitleAndDescriptionDetails(position, titleParam, descriptionParam);
    }

    public static NotificationImplementation impossibleRevokeCommand(
            InputPosition position, String titleParam, String descriptionParam) {
        return IMPOSSIBLE_REVOKE_COMMAND.notificationWithTitleAndDescriptionDetails(
                position, titleParam, descriptionParam);
    }

    private NotificationImplementation notification(InputPosition position, String... details) {
        return new NotificationImplementation.NotificationBuilder(this)
                .setPosition(position)
                .setNotificationDetails(details)
                .build();
    }

    private NotificationImplementation notificationWithMessage(
            InputPosition position, String[] oldDetails, String[] parameters) {
        return new NotificationImplementation.NotificationBuilder(this)
                .setPosition(position)
                .setNotificationDetails(oldDetails)
                .setMessageParameters(parameters)
                .build();
    }

    private NotificationImplementation notificationWithTitleDetails(InputPosition position, String... details) {
        return new NotificationImplementation.NotificationBuilder(this)
                .setPosition(position)
                .setTitleDetails(details)
                .build();
    }

    private NotificationImplementation notificationWithTitleAndDescriptionDetails(
            InputPosition position, String titleDetail, String... descriptionDetails) {
        // Allows a single detail in the title and multiple in the description
        return new NotificationImplementation.NotificationBuilder(this)
                .setPosition(position)
                .setTitleDetails(titleDetail)
                .setNotificationDetails(descriptionDetails)
                .build();
    }
}
