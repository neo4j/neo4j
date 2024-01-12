/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.notifications;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;

/**
 * This bundles a specific description with a (potentially) more generic NotificationCode.
 * <p>
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
                    + "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH (%s)",
            "The disconnected patterns `%s` build a cartesian product. A cartesian product may produce a large amount of "
                    + "data and slow down query processing."),
    RUNTIME_UNSUPPORTED(
            Status.Statement.RuntimeUnsupportedWarning,
            "Selected runtime is unsupported for this query, please use a different runtime instead or fallback to default. (%s)",
            NotificationCodeWithDescription.RUNTIME_UNSUPPORTED_MESSAGE),
    INDEX_HINT_UNFULFILLABLE(
            Status.Schema.HintedIndexNotFound,
            "The hinted index does not exist, please check the schema (%s)",
            "Unable to create a plan with `%s` because the index does not exist."),
    JOIN_HINT_UNFULFILLABLE(
            Status.Statement.JoinHintUnfulfillableWarning,
            "The hinted join was not planned. This could happen because no generated plan contained the join key, "
                    + "please try using a different join key or restructure your query. (%s)",
            "Unable to create a plan with `JOIN ON %s`. Try to change the join key(s) or restructure your query."),
    INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY(
            Status.Statement.DynamicProperty,
            "Using a dynamic property makes it impossible to use an index lookup for this query (%s)",
            "An index exists on label/type(s) `%s`. It is not possible to use indexes for dynamic "
                    + "properties. Consider using static properties."),
    DEPRECATED_FUNCTION_WITHOUT_REPLACEMENT(
            Status.Statement.FeatureDeprecationWarning,
            "The query used a deprecated function%s",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_2),

    DEPRECATED_FUNCTION_WITH_REPLACEMENT(
            Status.Statement.FeatureDeprecationWarning,
            "The query used a deprecated function%s",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_1),
    DEPRECATED_PROCEDURE_WITHOUT_REPLACEMENT(
            Status.Statement.FeatureDeprecationWarning,
            "The query used a deprecated procedure%s",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_2),

    DEPRECATED_PROCEDURE_WITH_REPLACEMENT(
            Status.Statement.FeatureDeprecationWarning,
            "The query used a deprecated procedure%s",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_1),

    DEPRECATED_RUNTIME_OPTION(
            Status.Statement.FeatureDeprecationWarning,
            "The query used a deprecated runtime option. (%s)",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_1),
    PROCEDURE_WARNING(
            Status.Procedure.ProcedureWarning,
            "The query used a procedure that generated a warning. (%s)",
            "The procedure `%s` generates the warning `%s`."),
    DEPRECATED_PROCEDURE_RETURN_FIELD(
            Status.Statement.FeatureDeprecationWarning,
            "The query used a deprecated field from a procedure. (%s)",
            "`%s` returned by procedure `%s` is deprecated."),
    DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR(
            Status.Statement.FeatureDeprecationWarning,
            "The semantics of using colon in the separation of alternative relationship types will change in a future version. (%s)",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_1),
    DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE(
            Status.Statement.FeatureDeprecationWarning,
            "The use of nodes or relationships for setting properties is deprecated and will be removed in a future version. "
                    + "Please use properties() instead.",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_1),

    DEPRECATED_PROPERTY_REFERENCE_IN_CREATE(
            Status.Statement.FeatureDeprecationWarning,
            // referencing that entity in a property definition in the same create clause
            "Creating an entity (%s) and referencing that entity in a property definition in the same CREATE is deprecated.",
            "Creating an entity (%s) and referencing that entity in a property definition in the same CREATE is deprecated."),

    DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP(
            Status.Statement.FeatureDeprecationWarning,
            "The use of shortestPath and allShortestPaths with fixed length relationships is deprecated and will be removed in a future version. "
                    + "Please use a path with a length of 1 [r*1..1] instead or a Match with a limit.",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_1),
    DEPRECATED_TEXT_INDEX_PROVIDER(
            Status.Statement.FeatureDeprecationWarning,
            "The `" + TextIndexProvider.DESCRIPTOR.name()
                    + "` provider for text indexes is deprecated and will be removed in a future version. "
                    + "Please use `" + TrigramIndexProvider.DESCRIPTOR.name() + "` instead.",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_1),

    DEPRECATED_IDENTIFIER_WHITESPACE_UNICODE(
            Status.Statement.FeatureDeprecationWarning,
            "The Unicode character `%s` is deprecated for unescaped identifiers and will be considered as a whitespace character in the future. "
                    + "To continue using it, escape the identifier by adding backticks around the identifier `%s`.",
            "The Unicode character `%s` is deprecated for unescaped identifiers and will be considered as a whitespace character in the future. "
                    + "To continue using it, escape the identifier by adding backticks around the identifier `%s`."),

    DEPRECATED_IDENTIFIER_UNICODE(
            Status.Statement.FeatureDeprecationWarning,
            "The character with the Unicode representation `%s` is deprecated for unescaped identifiers and will not be supported in the future. "
                    + "To continue using it, escape the identifier by adding backticks around the identifier `%s`.",
            "The character with the Unicode representation `%s` is deprecated for unescaped identifiers and will not be supported in the future. "
                    + "To continue using it, escape the identifier by adding backticks around the identifier `%s`."),
    EAGER_LOAD_CSV(
            Status.Statement.EagerOperator,
            "Using LOAD CSV with a large data set in a query where the execution plan contains the "
                    + "Eager operator could potentially consume a lot of memory and is likely to not perform well. "
                    + "See the Neo4j Manual entry on the Eager operator for more information and hints on "
                    + "how problems could be avoided.",
            "The query execution plan contains the `Eager` operator. `LOAD CSV` in combination with `Eager` "
                    + "can consume a lot of memory."),
    DEPRECATED_FORMAT(
            Status.Request.DeprecatedFormat,
            "The requested format has been deprecated. (%s)",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_1),
    LARGE_LABEL_LOAD_CSV(
            Status.Statement.NoApplicableIndex,
            "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely "
                    + "not perform well on large data sets. Please consider using a schema index.",
            "`LOAD CSV` in combination with `MATCH` or `MERGE` on a label that does not have an index may "
                    + "result in long execution times. Consider adding an index for label `%s`."),
    MISSING_LABEL(
            Status.Statement.UnknownLabelWarning,
            "One of the labels in your query is not available in the database, make sure you didn't "
                    + "misspell it or that the label is available when you run this statement in your application (%s)",
            "The label `%s` does not exist. Verify that the spelling is correct."),
    MISSING_REL_TYPE(
            Status.Statement.UnknownRelationshipTypeWarning,
            "One of the relationship types in your query is not available in the database, make sure you didn't "
                    + "misspell it or that the label is available when you run this statement in your application (%s)",
            "The relationship type `%s` does not exist. Verify that the spelling is correct."),
    MISSING_PROPERTY_NAME(
            Status.Statement.UnknownPropertyKeyWarning,
            "One of the property names in your query is not available in the database, make sure you didn't "
                    + "misspell it or that the label is available when you run this statement in your application (%s)",
            "The property `%s` does not exist. Verify that the spelling is correct."),
    UNBOUNDED_SHORTEST_PATH(
            Status.Statement.UnboundedVariableLengthPattern,
            "Using shortest path with an unbounded pattern will likely result in long execution times. "
                    + "It is recommended to use an upper limit to the number of node hops in your pattern.",
            "The provided pattern `%s` is unbounded. Shortest path with an unbounded pattern may result in long execution times. "
                    + "Use an upper limit (e.g. `[*..5]`) on the number of node hops in your pattern."),
    EXHAUSTIVE_SHORTEST_PATH(
            Status.Statement.ExhaustiveShortestPath,
            "Using shortest path with an exhaustive search fallback might cause query slow down since shortest path "
                    + "graph algorithms might not work for this use case. It is recommended to introduce a WITH to separate the "
                    + "MATCH containing the shortest path from the existential predicates on that path.",
            "The query runs with exhaustive shortest path due to the existential predicate(s) `%s`. "
                    + "It may be possible to use `WITH` to separate the `MATCH` from the existential predicate(s)."),
    MISSING_PARAMETERS_FOR_EXPLAIN(
            Status.Statement.ParameterNotProvided,
            "Did not supply query with enough parameters. The produced query plan will not be cached and is not executable without EXPLAIN. (%s)",
            "The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined parameter(s) `%s`. Provide the parameter(s)."),
    CODE_GENERATION_FAILED(
            Status.Statement.CodeGenerationFailed,
            "The database was unable to generate code for the query. A stacktrace can be found in the debug.log. (%s)",
            NotificationCodeWithDescription.RUNTIME_UNSUPPORTED_MESSAGE),

    SUBQUERY_VARIABLE_SHADOWING(
            Status.Statement.SubqueryVariableShadowing,
            "Variable in subquery is shadowing a variable with the same name from the outer scope. "
                    + "If you want to use that variable instead, it must be imported into the subquery using importing WITH clause. (%s)",
            "The variable `%s` in the subquery uses the same name as a variable from the outer query. Use "
                    + "`WITH %s` in the subquery to import the one from the outer scope unless you want it to be a "
                    + "new variable."),
    UNION_RETURN_ORDER(
            Status.Statement.FeatureDeprecationWarning,
            "All subqueries in a UNION [ALL] should have the same ordering for the return columns. "
                    + "Using differently ordered return items in a UNION [ALL] clause is deprecated and will be removed in a future version.",
            "All subqueries in a UNION [ALL] should have the same ordering for the return columns. "
                    + "Using differently ordered return items in a UNION [ALL] clause is deprecated and will be removed in a future version."),
    HOME_DATABASE_NOT_PRESENT(
            Status.Database.HomeDatabaseNotFound,
            "The home database provided does not currently exist in the DBMS. This command will not take effect until this database is created. (%s)",
            "The database `%s` does not exist. Verify that the spelling is correct or create the database for the command to take effect."),
    DEPRECATED_DATABASE_NAME(
            Status.Statement.FeatureDeprecationWarning,
            "Databases and aliases with unescaped `.` are deprecated unless to indicate that they belong to a composite database. "
                    + "Names containing `.` should be escaped. (%s)",
            "Databases and aliases with unescaped `.` are deprecated unless they belong to a composite database. "
                    + "Names containing `.` should be escaped. (%s)"),
    UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION(
            Status.Statement.UnsatisfiableRelationshipTypeExpression,
            "Relationship type expression cannot possibly be satisfied. (%s)",
            "The expression `%s` cannot be satisfied because relationships must have exactly one type."),
    REPEATED_RELATIONSHIP_REFERENCE(
            Status.Statement.RepeatedRelationshipReference,
            "A relationship is referenced more than once in the query, which leads to no results because"
                    + " relationships must not occur more than once in each result. (%s)",
            "`%s` is repeated in `%s`, which leads to no results."),
    REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE(
            Status.Statement.RepeatedRelationshipReference,
            "A variable-length relationship variable is bound more than once, which leads to no results because"
                    + " relationships must not occur more than once in each result. (%s)",
            "`%s` is repeated in `%s`, which leads to no results."),
    DEPRECATED_CONNECT_COMPONENTS_PLANNER_PRE_PARSER_OPTION(
            Status.Statement.FeatureDeprecationWarning,
            "The Cypher query option `connectComponentsPlanner` is deprecated and will be removed without a replacement. "
                    + "The product's default behavior of using a cost-based IDP search algorithm when combining sub-plans will be kept. "
                    + "For more information, see Cypher Manual -> Cypher planner.",
            NotificationCodeWithDescription.DEPRECATION_MESSAGE_2),
    COMMAND_HAS_NO_EFFECT_ASSIGN_PRIVILEGE(
            Status.Security.CommandHasNoEffect,
            "The role already has the privilege. See Status Codes documentation for more information.",
            "`%s` has no effect. The role already has the privilege."),

    COMMAND_HAS_NO_EFFECT_REVOKE_PRIVILEGE(
            Status.Security.CommandHasNoEffect,
            "The role does not have the privilege. See Status Codes documentation for more information.",
            "`%s` has no effect. The role does not have the privilege."),

    COMMAND_HAS_NO_EFFECT_GRANT_ROLE(
            Status.Security.CommandHasNoEffect,
            "The user already has the role. See Status Codes documentation for more information.",
            "`%s` has no effect. The user already has the role."),

    COMMAND_HAS_NO_EFFECT_REVOKE_ROLE(
            Status.Security.CommandHasNoEffect,
            "The user does not have the role. See Status Codes documentation for more information.",
            "`%s` has no effect. The user does not have the role."),

    IMPOSSIBLE_REVOKE_COMMAND(
            Status.Security.ImpossibleRevokeCommand,
            "%s Make sure nothing is misspelled. This notification will become an error in a future major version. "
                    + "See Status Codes documentation for more information.",
            "`%s` has no effect. %s Make sure nothing is misspelled. This notification will become an error in a future major version."),

    SERVER_ALREADY_ENABLED(
            Status.Cluster.ServerAlreadyEnabled,
            "Server `%s` is already enabled. Verify that this is the intended server.",
            "`ENABLE SERVER` has no effect. Server `%s` is already enabled. Verify that this is the intended server."),

    SERVER_ALREADY_CORDONED(
            Status.Cluster.ServerAlreadyCordoned,
            "Server `%s` is already cordoned. Verify that this is the intended server.",
            "`CORDON SERVER` has no effect. Server `%s` is already cordoned. Verify that this is the intended server."),

    NO_DATABASES_REALLOCATED(
            Status.Cluster.NoDatabasesReallocated,
            "No databases were reallocated. No better allocation is currently possible.",
            "`REALLOCATE DATABASES` has no effect. No databases were reallocated. No better allocation is currently possible."),

    CORDONED_SERVERS_EXISTED_DURING_ALLOCATION(
            Status.Cluster.CordonedServersExistedDuringAllocation,
            "Server(s) `%s` are cordoned. This can impact allocation decisions.",
            "Cordoned servers existed when making an allocation decision. Server(s) `%s` are cordoned. This can impact allocation decisions."),

    REQUESTED_TOPOLOGY_MATCHED_CURRENT_TOPOLOGY(
            Status.Cluster.RequestedTopologyMatchedCurrentTopology,
            "The requested topology matched the current topology. No allocations were changed.",
            "`ALTER DATABASE` has no effect. The requested topology matched the current topology. No allocations were changed.");

    private final Status status;
    private final String description;
    private final String message;

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

    public static NotificationImplementation cartesianProduct(
            InputPosition position, String oldDetail, String pattern) {
        return CARTESIAN_PRODUCT.notificationWithParameters(position, new String[] {oldDetail}, new String[] {pattern});
    }

    public static NotificationImplementation runtimeUnsupported(
            InputPosition position, String failingRuntimeConf, String fallbackRuntimeConf, String cause) {
        final var oldDetails = new String[] {cause};
        final var params = new String[] {failingRuntimeConf, fallbackRuntimeConf, cause};
        return RUNTIME_UNSUPPORTED.notificationWithParameters(position, oldDetails, params);
    }

    public static NotificationImplementation indexHintUnfulfillable(
            InputPosition position, String oldDetail, String indexes) {
        return INDEX_HINT_UNFULFILLABLE.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {indexes});
    }

    public static NotificationImplementation joinHintUnfulfillable(
            InputPosition position, String oldDetail, String variableNames) {
        return JOIN_HINT_UNFULFILLABLE.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {variableNames});
    }

    public static NotificationImplementation indexLookupForDynamicProperty(
            InputPosition position, String oldDetails, String parameters) {
        return INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notificationWithParameters(
                position, new String[] {oldDetails}, new String[] {parameters});
    }

    public static NotificationImplementation deprecatedFunctionWithoutReplacement(
            InputPosition position, String param, String oldName) {
        return DEPRECATED_FUNCTION_WITHOUT_REPLACEMENT.notificationWithParameters(
                position, new String[] {param}, new String[] {oldName});
    }

    public static NotificationImplementation deprecatedFunctionWithReplacement(
            InputPosition position, String param, String oldName, String newName) {
        return DEPRECATED_FUNCTION_WITH_REPLACEMENT.notificationWithParameters(
                position, new String[] {param}, new String[] {oldName, newName});
    }

    public static NotificationImplementation deprecatedProcedureWithoutReplacement(
            InputPosition position, String param, String oldName) {
        return DEPRECATED_PROCEDURE_WITHOUT_REPLACEMENT.notificationWithParameters(
                position, new String[] {param}, new String[] {oldName});
    }

    public static NotificationImplementation deprecatedProcedureWithReplacement(
            InputPosition position, String param, String oldName, String newName) {
        return DEPRECATED_PROCEDURE_WITH_REPLACEMENT.notificationWithParameters(
                position, new String[] {param}, new String[] {oldName, newName});
    }

    public static NotificationImplementation deprecatedRuntimeOption(
            InputPosition position, String param, String oldOption, String newOption) {
        return DEPRECATED_RUNTIME_OPTION.notificationWithParameters(
                position, new String[] {param}, new String[] {oldOption, newOption});
    }

    public static NotificationImplementation procedureWarning(
            InputPosition position, String parameter, String warning, String procedure) {
        return PROCEDURE_WARNING.notificationWithParameters(
                position, new String[] {parameter}, new String[] {procedure, warning});
    }

    public static NotificationImplementation deprecatedProcedureReturnField(
            InputPosition position, String param, String procedure, String field) {
        return DEPRECATED_PROCEDURE_RETURN_FIELD.notificationWithParameters(
                position, new String[] {param}, new String[] {field, procedure});
    }

    public static NotificationImplementation deprecatedRelationshipTypeSeparator(
            InputPosition position, String param, String deprecated, String replacement) {
        return DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notificationWithParameters(
                position, new String[] {param}, new String[] {deprecated, replacement});
    }

    public static NotificationImplementation deprecatedNodeOrRelationshipOnRhsSetClause(
            InputPosition position, String deprecated, String replacement) {
        return DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE.notificationWithParameters(
                position, new String[] {}, new String[] {deprecated, replacement});
    }

    public static NotificationImplementation deprecatedPropertyReferenceInCreate(InputPosition position, String param) {
        return DEPRECATED_PROPERTY_REFERENCE_IN_CREATE.notificationWithParameters(
                position, new String[] {param}, new String[] {param});
    }

    public static NotificationImplementation deprecatedShortestPathWithFixedLengthRelationship(
            InputPosition position, String deprecated, String replacement) {
        return DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP.notificationWithParameters(
                position, new String[] {}, new String[] {deprecated, replacement});
    }

    public static NotificationImplementation deprecatedTextIndexProvider(InputPosition position) {
        return DEPRECATED_TEXT_INDEX_PROVIDER.notificationWithParameters(position, new String[] {}, new String[] {
            TextIndexProvider.DESCRIPTOR.name(), TrigramIndexProvider.DESCRIPTOR.name()
        });
    }

    public static NotificationImplementation deprecatedIdentifierWhitespaceUnicode(
            InputPosition position, Character unicode, String identifier) {
        String formattedUnicode = String.format("\\u%04x", (int) unicode);
        return DEPRECATED_IDENTIFIER_WHITESPACE_UNICODE.notificationWithParameters(
                position, new String[] {formattedUnicode, identifier}, new String[] {formattedUnicode, identifier});
    }

    public static NotificationImplementation deprecatedIdentifierUnicode(
            InputPosition position, Character unicode, String identifier) {
        String formattedUnicode = String.format("\\u%04x", (int) unicode);
        return DEPRECATED_IDENTIFIER_UNICODE.notificationWithParameters(
                position, new String[] {formattedUnicode, identifier}, new String[] {formattedUnicode, identifier});
    }

    public static NotificationImplementation eagerLoadCsv(InputPosition position) {
        return EAGER_LOAD_CSV.notification(position);
    }

    public static NotificationImplementation deprecatedFormat(
            InputPosition position, String oldDetail, String deprecatedFormat, String newFormat) {
        return DEPRECATED_FORMAT.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {deprecatedFormat, newFormat});
    }

    public static NotificationImplementation largeLabelLoadCsv(InputPosition position, String labelName) {
        return LARGE_LABEL_LOAD_CSV.notificationWithParameters(position, new String[] {}, new String[] {labelName});
    }

    public static NotificationImplementation missingLabel(InputPosition position, String oldDetail, String labelName) {
        return MISSING_LABEL.notificationWithParameters(position, new String[] {oldDetail}, new String[] {labelName});
    }

    public static NotificationImplementation missingRelType(InputPosition position, String oldDetail, String relType) {
        return MISSING_REL_TYPE.notificationWithParameters(position, new String[] {oldDetail}, new String[] {relType});
    }

    public static NotificationImplementation missingPropertyName(
            InputPosition position, String oldDetails, String property) {
        return MISSING_PROPERTY_NAME.notificationWithParameters(
                position, new String[] {oldDetails}, new String[] {property});
    }

    public static NotificationImplementation unboundedShortestPath(InputPosition position, String pattern) {
        return UNBOUNDED_SHORTEST_PATH.notificationWithParameters(position, new String[] {}, new String[] {pattern});
    }

    public static NotificationImplementation exhaustiveShortestPath(InputPosition position, String pathPredicates) {
        return EXHAUSTIVE_SHORTEST_PATH.notificationWithParameters(
                position, new String[] {}, new String[] {pathPredicates});
    }

    public static NotificationImplementation missingParameterForExplain(
            InputPosition position, String oldDetails, String parameters) {
        return MISSING_PARAMETERS_FOR_EXPLAIN.notificationWithParameters(
                position, new String[] {oldDetails}, new String[] {parameters});
    }

    public static NotificationImplementation codeGenerationFailed(
            InputPosition position, String failingRuntimeConf, String fallbackRuntimeConf, String cause) {
        final var oldDetails = new String[] {cause};
        final var params = new String[] {failingRuntimeConf, fallbackRuntimeConf, cause};
        return CODE_GENERATION_FAILED.notificationWithParameters(position, oldDetails, params);
    }

    public static NotificationImplementation subqueryVariableShadowing(
            InputPosition position, String oldDetail, String variable) {
        return SUBQUERY_VARIABLE_SHADOWING.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {variable, variable});
    }

    public static NotificationImplementation unionReturnOrder(InputPosition position) {
        return UNION_RETURN_ORDER.notification(position);
    }

    public static NotificationImplementation homeDatabaseNotPresent(
            InputPosition position, String oldDetail, String missingDb) {
        return HOME_DATABASE_NOT_PRESENT.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {missingDb});
    }

    public static NotificationImplementation deprecatedDatabaseName(InputPosition position, String param) {
        return DEPRECATED_DATABASE_NAME.notificationWithParameters(
                position, new String[] {param}, new String[] {param});
    }

    public static NotificationImplementation unsatisfiableRelationshipTypeExpression(
            InputPosition position, String param, String expression) {
        return UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION.notificationWithParameters(
                position, new String[] {param}, new String[] {expression});
    }

    public static NotificationImplementation repeatedRelationshipReference(
            InputPosition position, String param, String relName, String pattern) {
        return REPEATED_RELATIONSHIP_REFERENCE.notificationWithParameters(
                position, new String[] {param}, new String[] {relName, pattern});
    }

    public static NotificationImplementation repeatedVarLengthRelationshipReference(
            InputPosition position, String param, String relName, String pattern) {
        return REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE.notificationWithParameters(
                position, new String[] {param}, new String[] {relName, pattern});
    }

    public static NotificationImplementation deprecatedConnectComponentsPlannerPreParserOption(InputPosition position) {
        return DEPRECATED_CONNECT_COMPONENTS_PLANNER_PRE_PARSER_OPTION.notificationWithParameters(
                position, new String[] {}, new String[] {"connectComponentsPlanner"});
    }

    public static NotificationImplementation commandHasNoEffectAssignPrivilege(
            InputPosition position, String titleParam) {
        return COMMAND_HAS_NO_EFFECT_ASSIGN_PRIVILEGE.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {}, new String[] {titleParam});
    }

    public static NotificationImplementation commandHasNoEffectRevokePrivilege(
            InputPosition position, String titleParam) {
        return COMMAND_HAS_NO_EFFECT_REVOKE_PRIVILEGE.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {}, new String[] {titleParam});
    }

    public static NotificationImplementation commandHasNoEffectGrantRole(InputPosition position, String titleParam) {
        return COMMAND_HAS_NO_EFFECT_GRANT_ROLE.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {}, new String[] {titleParam});
    }

    public static NotificationImplementation commandHasNoEffectRevokeRole(InputPosition position, String titleParam) {
        return COMMAND_HAS_NO_EFFECT_REVOKE_ROLE.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {}, new String[] {titleParam});
    }

    public static NotificationImplementation impossibleRevokeCommand(
            InputPosition position, String titleParam, String descriptionParam) {
        return IMPOSSIBLE_REVOKE_COMMAND.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {descriptionParam}, new String[] {titleParam, descriptionParam});
    }

    public static NotificationImplementation serverAlreadyEnabled(InputPosition position, String server) {
        return SERVER_ALREADY_ENABLED.notificationWithParameters(
                position, new String[] {server}, new String[] {server});
    }

    public static NotificationImplementation serverAlreadyCordoned(InputPosition position, String server) {
        return SERVER_ALREADY_CORDONED.notificationWithParameters(
                position, new String[] {server}, new String[] {server});
    }

    public static NotificationImplementation noDatabasesReallocated(InputPosition position) {
        return NO_DATABASES_REALLOCATED.notification(position);
    }

    public static NotificationImplementation cordonedServersExist(InputPosition position, String servers) {
        return CORDONED_SERVERS_EXISTED_DURING_ALLOCATION.notificationWithParameters(
                position, new String[] {servers}, new String[] {servers});
    }

    public static NotificationImplementation requestedTopologyMatchedCurrentTopology(InputPosition position) {
        return REQUESTED_TOPOLOGY_MATCHED_CURRENT_TOPOLOGY.notification(position);
    }

    private NotificationImplementation notification(InputPosition position) {
        return notificationWithParameters(position, new String[] {}, new String[] {});
    }

    private NotificationImplementation notificationWithParameters(
            InputPosition position, String[] oldDetails, String[] parameters) {
        return new NotificationImplementation.NotificationBuilder(this)
                .setPosition(position)
                .setNotificationDetails(oldDetails)
                .setMessageParameters(parameters)
                .build();
    }

    private NotificationImplementation notificationWithTitleAndDescriptionDetails(
            InputPosition position, String titleDetail, String[] descriptionDetails, String[] parameters) {
        // Allows a single detail in the title and multiple in the description
        return new NotificationImplementation.NotificationBuilder(this)
                .setPosition(position)
                .setTitleDetails(titleDetail)
                .setNotificationDetails(descriptionDetails)
                .setMessageParameters(parameters)
                .build();
    }

    private static final String DEPRECATION_MESSAGE_1 = "`%s` is deprecated. It is replaced by `%s`.";

    private static final String DEPRECATION_MESSAGE_2 = "`%s` is deprecated and will be removed without a replacement.";

    private static final String RUNTIME_UNSUPPORTED_MESSAGE =
            "The query cannot be executed with `%s`, `%s` is used. Cause: `%s`.";
}
