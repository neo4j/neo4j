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

import java.util.List;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfo;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.gqlstatus.SimpleMessageFormatter;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.storageengine.api.DeprecatedFormatWarning;

/**
 * This bundles a specific description with a (potentially) more generic NotificationCode.
 * <p>
 * If changing or adding a notification, please make sure you follow the guidelines here: <a href="https://github.com/neo4j/docs-status-codes/blob/dev/README.adoc">NOTIFICATION GUIDELINES</a>
 */
public enum NotificationCodeWithDescription {
    CARTESIAN_PRODUCT(
            Status.Statement.CartesianProduct,
            GqlStatusInfoCodes.STATUS_03N90,
            "If a part of a query contains multiple disconnected patterns, this will build a "
                    + "cartesian product between all those parts. This may produce a large amount of data and slow down"
                    + " query processing. "
                    + "While occasionally intended, it may often be possible to reformulate the query that avoids the "
                    + "use of this cross "
                    + "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH (%s)"),
    RUNTIME_UNSUPPORTED(
            Status.Statement.RuntimeUnsupportedWarning,
            GqlStatusInfoCodes.STATUS_01N40,
            "Selected runtime is unsupported for this query, please use a different runtime instead or fallback to default. (%s)"),
    INDEX_HINT_UNFULFILLABLE(
            Status.Schema.HintedIndexNotFound,
            GqlStatusInfoCodes.STATUS_01N31,
            "The hinted index does not exist, please check the schema (%s)"),
    JOIN_HINT_UNFULFILLABLE(
            Status.Statement.JoinHintUnfulfillableWarning,
            GqlStatusInfoCodes.STATUS_01N30,
            "The hinted join was not planned. This could happen because no generated plan contained the join key, "
                    + "please try using a different join key or restructure your query. (%s)"),
    INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY(
            Status.Statement.DynamicProperty,
            GqlStatusInfoCodes.STATUS_03N95,
            "Using a dynamic property makes it impossible to use an index lookup for this query (%s)"),
    DEPRECATED_FUNCTION_WITHOUT_REPLACEMENT(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N02,
            "The query used a deprecated function%s"),

    DEPRECATED_FUNCTION_WITH_REPLACEMENT(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "The query used a deprecated function%s"),
    DEPRECATED_PROCEDURE_WITHOUT_REPLACEMENT(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N02,
            "The query used a deprecated procedure%s"),

    DEPRECATED_PROCEDURE_WITH_REPLACEMENT(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "The query used a deprecated procedure%s"),

    DEPRECATED_RUNTIME_OPTION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "The query used a deprecated runtime option. (%s)"),
    PROCEDURE_WARNING(
            Status.Procedure.ProcedureWarning,
            GqlStatusInfoCodes.STATUS_01N62,
            "The query used a procedure that generated a warning. (%s)"),
    DEPRECATED_PROCEDURE_FIELD(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "The procedure has a deprecated field. (%s)"),
    DEPRECATED_FUNCTION_FIELD(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "The function has a deprecated field. (%s)"),
    DEPRECATED_PROCEDURE_NAMESPACE(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "The namespace of the called user-defined procedure is deprecated. (%s)"),
    DEPRECATED_FUNCTION_NAMESPACE(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "The namespace of the invoked user-defined function is deprecated. (%s)"),
    SHADOWING_INTERNAL_FUNCTION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "The namespace of the invoked user-defined function is deprecated and the function is shadowing an internal function. (%s)"),
    DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "The semantics of using colon in the separation of alternative relationship types will change in a future version. (%s)"),
    DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "The use of nodes or relationships for setting properties is deprecated and will be removed in a future version. "
                    + "Please use properties() instead."),

    DEPRECATED_PROPERTY_REFERENCE_IN_CREATE(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            // referencing that entity in a property definition in the same create clause
            "Creating an entity (%s) and referencing that entity in a property definition in the same CREATE is deprecated."),

    DEPRECATED_PROPERTY_REFERENCE_IN_MERGE(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            // referencing that entity in a property definition in the same merge clause
            "Merging an entity (%s) and referencing that entity in a property definition in the same MERGE is deprecated."),

    DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "The use of shortestPath and allShortestPaths with fixed length relationships is deprecated and will be removed in a future version. "
                    + "Please use a path with a length of 1 [r*1..1] instead or a Match with a limit."),
    DEPRECATED_TEXT_INDEX_PROVIDER(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "`" + AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR.name()
                    + "`, `" + AllIndexProviderDescriptors.TEXT_V2_DESCRIPTOR.name()
                    + "` providers for text indexes are deprecated and will be removed in a future version. "
                    + "Please use `" + AllIndexProviderDescriptors.TEXT_V3_DESCRIPTOR.name() + "` instead."),

    DEPRECATED_INDEX_PROVIDER_OPTION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "The `indexProvider` option is deprecated and will be removed in a future version. "
                    + "Neo4j does not use the given option but instead selects the most performant index provider available."),

    DEPRECATED_IDENTIFIER_WHITESPACE_UNICODE(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "The Unicode character `%s` is deprecated for unescaped identifiers and will be considered as a whitespace character in the future. "
                    + "To continue using it, escape the identifier by adding backticks around the identifier `%s`."),

    DEPRECATED_IDENTIFIER_UNICODE(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "The character with the Unicode representation `%s` is deprecated for unescaped identifiers and will not be supported in the future. "
                    + "To continue using it, escape the identifier by adding backticks around the identifier `%s`."),

    EAGER_LOAD_CSV(
            Status.Statement.EagerOperator,
            GqlStatusInfoCodes.STATUS_03N94,
            "Using LOAD CSV with a large data set in a query where the execution plan contains the "
                    + "Eager operator could potentially consume a lot of memory and is likely to not perform well. "
                    + "See the Neo4j Manual entry on the Eager operator for more information and hints on "
                    + "how problems could be avoided."),
    VIRTUAL_GRAPH_POST_PROCESSING(
            Status.Statement.VirtualGraphPostProcessing,
            GqlStatusInfoCodes.STATUS_03N97,
            GqlStatusInfoCodes.STATUS_03N97.getTemplate()),
    DEPRECATED_FORMAT(
            Status.Request.DeprecatedFormat,
            GqlStatusInfoCodes.STATUS_01N01,
            "The requested format has been deprecated. (%s)"),

    DEPRECATED_REQUESTED_FEATURE(
            Status.Request.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "%s is deprecated. It is replaced by %s."),

    LARGE_LABEL_LOAD_CSV(
            Status.Statement.NoApplicableIndex,
            GqlStatusInfoCodes.STATUS_03N93,
            "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely "
                    + "not perform well on large data sets. Please consider using a schema index."),
    MISSING_LABEL(
            Status.Statement.UnknownLabelWarning,
            GqlStatusInfoCodes.STATUS_01N50,
            "One of the labels in your query is not available in the database, make sure you didn't "
                    + "misspell it or that the label is available when you run this statement in your application (%s)"),
    MISSING_REL_TYPE(
            Status.Statement.UnknownRelationshipTypeWarning,
            GqlStatusInfoCodes.STATUS_01N51,
            "One of the relationship types in your query is not available in the database, make sure you didn't "
                    + "misspell it or that the label is available when you run this statement in your application (%s)"),
    MISSING_PROPERTY_NAME(
            Status.Statement.UnknownPropertyKeyWarning,
            GqlStatusInfoCodes.STATUS_01N52,
            "One of the property names in your query is not available in the database, make sure you didn't "
                    + "misspell it or that the label is available when you run this statement in your application (%s)"),
    UNBOUNDED_SHORTEST_PATH(
            Status.Statement.UnboundedVariableLengthPattern,
            GqlStatusInfoCodes.STATUS_03N91,
            "Using shortest path with an unbounded pattern will likely result in long execution times. "
                    + "It is recommended to use an upper limit to the number of node hops in your pattern."),
    EXHAUSTIVE_SHORTEST_PATH(
            Status.Statement.ExhaustiveShortestPath,
            GqlStatusInfoCodes.STATUS_03N92,
            "Using shortest path with an exhaustive search fallback might cause query slow down since shortest path "
                    + "graph algorithms might not work for this use case. It is recommended to introduce a WITH to separate the "
                    + "MATCH containing the shortest path from the existential predicates on that path."),
    MISSING_PARAMETERS_FOR_EXPLAIN(
            Status.Statement.ParameterNotProvided,
            GqlStatusInfoCodes.STATUS_01N60,
            "Did not supply query with enough parameters. The produced query plan will not be cached and is not executable without EXPLAIN. (%s)"),
    CODE_GENERATION_FAILED(
            Status.Statement.CodeGenerationFailed,
            GqlStatusInfoCodes.STATUS_03N96,
            "The database was unable to generate code for the query. A stacktrace can be found in the debug.log. (%s)"),
    SUBQUERY_VARIABLE_SHADOWING(
            Status.Statement.SubqueryVariableShadowing,
            GqlStatusInfoCodes.STATUS_03N60,
            "Variable in subquery is shadowing a variable with the same name from the outer scope. "
                    + "If you want to use that variable instead, it must be imported into the subquery using a variable scope clause. (%s)"),
    REDUNDANT_OPTIONAL_PROCEDURE(
            Status.Statement.RedundantOptionalProcedure,
            GqlStatusInfoCodes.STATUS_03N61,
            "The use of `OPTIONAL` is redundant as `CALL %s` is a void procedure."),
    REDUNDANT_OPTIONAL_SUBQUERY(
            Status.Statement.RedundantOptionalSubquery,
            GqlStatusInfoCodes.STATUS_03N62,
            "The use of `OPTIONAL` is redundant as `CALL` is a unit subquery."),
    DEPRECATED_IMPORTING_WITH_IN_SUBQUERY_CALL(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "%s subquery without a variable scope clause is deprecated. " + "Use %s (%s) { ... }"),
    DEPRECATED_COMPLEX_SUBCLAUSE_EXPRESSION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "The subclause expression `%s` contains a reference to the complex projection item expression `%s` and is deprecated. It is replaced by referencing the projection item expression by an alias."),
    DEPRECATED_AMBIGUOUS_SUBCLAUSE_EXPRESSION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "The subclause expression `%s` contains an ambiguous reference to variable `%s` and is deprecated. It is replaced by referencing the projection item expression by an alias or using the `GROUP BY` clause."),
    DEPRECATED_AMBIGUOUS_AND_COMPLEX_SUBCLAUSE_EXPRESSION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "The subclause expression `%s` contains an ambiguous reference to variable `%s` and a reference to the complex projection item expression `%s` and is deprecated. It is replaced by referencing the projection item expression by an alias or using the `GROUP BY` clause."),
    DEPRECATED_WHERE_VARIABLE_IN_NODE_PATTERN(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "'%s' is deprecated. It is replaced by '%s'."),
    DEPRECATED_WHERE_VARIABLE_IN_RELATIONSHIP_PATTERN(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "'%s' is deprecated. It is replaced by '%s'."),
    DEPRECATED_PRECEDENCE_OF_LABEL_EXPRESSION_PREDICATED(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "'%s' is deprecated. It is replaced by '%s'."),
    DEPRECATED_KEYWORD_VARIABLE_IN_WHEN_OPERAND(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "'%s' is deprecated. It is replaced by '%s'."),
    HOME_DATABASE_NOT_PRESENT(
            Status.Database.HomeDatabaseNotFound,
            GqlStatusInfoCodes.STATUS_00N50,
            "The home database provided does not currently exist in the DBMS. This command will not take effect until this database is created. (%s)"),
    DEPRECATED_QUOTED_GRAPH_REFERENCE(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "Graph references with separately backticked name parts (%s) are deprecated. In future Cypher versions, use parameters or backtick the entire name (%s)."),
    DEPRECATED_QUOTED_GRAPH_BY_NAME_ARGUMENT(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            "Graph references with separately backticked name parts (%s) are deprecated. In future Cypher versions, remove the backticks (%s)."),
    UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION(
            Status.Statement.UnsatisfiableRelationshipTypeExpression,
            GqlStatusInfoCodes.STATUS_01N61,
            "Relationship type expression cannot possibly be satisfied. (%s)"),
    REPEATED_RELATIONSHIP_REFERENCE(
            Status.Statement.RepeatedRelationshipReference,
            GqlStatusInfoCodes.STATUS_01N63,
            "A relationship is referenced more than once in the query, which leads to no results because"
                    + " relationships must not occur more than once in each result. (%s)"),
    REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE(
            Status.Statement.RepeatedRelationshipReference,
            GqlStatusInfoCodes.STATUS_01N63,
            "A variable-length relationship variable is bound more than once, which leads to no results because"
                    + " relationships must not occur more than once in each result. (%s)"),
    DEPRECATED_CONNECT_COMPONENTS_PLANNER_PRE_PARSER_OPTION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N02,
            "The Cypher query option `connectComponentsPlanner` is deprecated and will be removed without a replacement. "
                    + "The product's default behavior of using a cost-based IDP search algorithm when combining sub-plans will be kept. "
                    + "For more information, see Cypher Manual -> Cypher planner."),

    DEPRECATED_EAGER_ANALYZER_PRE_PARSER_OPTION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N02,
            "The Cypher query option `eagerAnalyzer` is deprecated. "
                    + "It will be removed without a replacement. "
                    + "The option is ignored, eagerness analysis is systematically performed on the logical plan "
                    + "regardless of the value provided."),

    RETIRED_PLANNER_VERSION_PRE_PARSER_OPTION(
            Status.Statement.PlannerVersionUnsupportedWarning,
            GqlStatusInfoCodes.STATUS_01N84,
            "The Cypher planner version %s is no longer supported. The default planner version is used instead."),

    COMMAND_HAS_NO_EFFECT_ASSIGN_PRIVILEGE(
            Status.Security.CommandHasNoEffect,
            GqlStatusInfoCodes.STATUS_00N70,
            "The role already has the privilege. See Status Codes documentation for more information."),

    COMMAND_HAS_NO_EFFECT_REVOKE_PRIVILEGE(
            Status.Security.CommandHasNoEffect,
            GqlStatusInfoCodes.STATUS_00N71,
            "The role does not have the privilege. See Status Codes documentation for more information."),

    COMMAND_HAS_NO_EFFECT_GRANT_ROLE(
            Status.Security.CommandHasNoEffect,
            GqlStatusInfoCodes.STATUS_00N70,
            "The user already has the role. See Status Codes documentation for more information."),

    COMMAND_HAS_NO_EFFECT_REVOKE_ROLE(
            Status.Security.CommandHasNoEffect,
            GqlStatusInfoCodes.STATUS_00N71,
            "The user does not have the role. See Status Codes documentation for more information."),

    COMMAND_HAS_NO_EFFECT_GRANT_ROLE_TO_AUTH_RULE(
            Status.Security.CommandHasNoEffect,
            GqlStatusInfoCodes.STATUS_00N70,
            "The auth rule already has the role. See Status Codes documentation for more information."),

    COMMAND_HAS_NO_EFFECT_REVOKE_ROLE_TO_AUTH_RULE(
            Status.Security.CommandHasNoEffect,
            GqlStatusInfoCodes.STATUS_00N71,
            "The auth rule does not have the role. See Status Codes documentation for more information."),

    AUTH_PROVIDER_NOT_DEFINED(
            Status.Security.AuthProviderNotDefined,
            GqlStatusInfoCodes.STATUS_00N72,
            "The auth provider `%s` is not defined in the configuration. "
                    + "Verify that the spelling is correct or define `%s` in the configuration."),

    IMPOSSIBLE_REVOKE_COMMAND(
            Status.Security.ImpossibleRevokeCommand,
            GqlStatusInfoCodes.STATUS_01N70,
            "%s Make sure nothing is misspelled. This notification will become an error in a future major version. "
                    + "See Status Codes documentation for more information."),

    EXTERNAL_AUTH_NOT_ENABLED(
            Status.Security.ExternalAuthNotEnabled,
            GqlStatusInfoCodes.STATUS_01N71,
            "Use setting `dbms.security.require_local_user` to enable external auth."),

    SHARDED_PRIVILEGE_PERFORMANCE(
            Status.Security.ShardedPrivilegePerformance,
            GqlStatusInfoCodes.STATUS_01N73,
            "This specified privilege severely reduces the performance of queries run on sharded databases. Consider excluding sharded databases for now."),

    OIDC_CREDENTIAL_FORWARDING_NOT_ENABLED(
            Status.Security.OidcCredentialForwardingNotEnabled,
            GqlStatusInfoCodes.STATUS_01N74,
            "Use setting `dbms.security.allow_oidc_credential_forwarding_enabled` to enable OIDC credential forwarding."),

    SERVER_ALREADY_ENABLED(
            Status.Cluster.ServerAlreadyEnabled,
            GqlStatusInfoCodes.STATUS_00N80,
            "Server `%s` is already enabled. Verify that this is the intended server."),

    SERVER_ALREADY_CORDONED(
            Status.Cluster.ServerAlreadyCordoned,
            GqlStatusInfoCodes.STATUS_00N81,
            "Server `%s` is already cordoned. Verify that this is the intended server."),

    NO_DATABASES_REALLOCATED(
            Status.Cluster.NoDatabasesReallocated,
            GqlStatusInfoCodes.STATUS_00N82,
            "No databases were reallocated. No better allocation is currently possible."),

    CORDONED_SERVERS_EXISTED_DURING_ALLOCATION(
            Status.Cluster.CordonedServersExistedDuringAllocation,
            GqlStatusInfoCodes.STATUS_00N83,
            "Server(s) `%s` are cordoned. This can impact allocation decisions."),

    REQUESTED_TOPOLOGY_MATCHED_CURRENT_TOPOLOGY(
            Status.Cluster.RequestedTopologyMatchedCurrentTopology,
            GqlStatusInfoCodes.STATUS_00N84,
            "The requested topology matched the current topology. No allocations were changed."),

    INDEX_OR_CONSTRAINT_ALREADY_EXISTS(
            Status.Schema.IndexOrConstraintAlreadyExists, GqlStatusInfoCodes.STATUS_00NA0, "`%s` already exists."),

    INDEX_OR_CONSTRAINT_DOES_NOT_EXIST(
            Status.Schema.IndexOrConstraintDoesNotExist, GqlStatusInfoCodes.STATUS_00NA1, "`%s` does not exist."),

    VECTOR_INDEX_DIMENSIONS_NOT_SPECIFIED(
            Status.Schema.VectorIndexDimensionsNotSpecified,
            GqlStatusInfoCodes.STATUS_00NA2,
            "When creating a vector index, `vector.dimensions` should be specified. Omitting it is allowed, but specifying dimensions ensures that only vectors of that size are indexed and makes dimension mismatches fail clearly at query time. For example, set `OPTIONS { indexConfig: { `vector.dimensions`: 1536 } }` when creating the index."),

    AGGREGATION_SKIPPED_NULL(
            Status.Statement.AggregationSkippedNull,
            GqlStatusInfoCodes.STATUS_01G11,
            "null value eliminated in set function."),
    DEPRECATED_BOOLEAN_COERCION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N02,
            "The query converted a list or path to a boolean value."),
    INSECURE_PROTOCOL(
            Status.Statement.InsecureProtocol, GqlStatusInfoCodes.STATUS_01N72, "Query uses an insecure protocol."),
    DEPRECATED_OPTION_IN_OPTION_MAP(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N01,
            "`%s` is deprecated. It is replaced by `%s`."),
    DEPRECATED_SEEDING_OPTION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N02,
            "`%s` is deprecated. Credentials are now supplied via the cloud provider mechanisms."),
    DEPRECATED_EXISTING_DATA_OPTION(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N02,
            "`%s` is deprecated. Use of existing data is implicit with seeding."),
    DEPRECATED_STORE_FORMAT(
            Status.Statement.FeatureDeprecationWarning,
            GqlStatusInfoCodes.STATUS_01N00,
            DeprecatedFormatWarning.getTargetFormatWarning("%s")),

    WAIT_SERVER_UNAVAILABLE(
            Status.Cluster.ServerNotAvailable, GqlStatusInfoCodes.STATUS_01N82, "Server `%s` is not available."),

    WAIT_SERVER_CATCHING_UP(
            Status.Cluster.ServerCatchingUp,
            GqlStatusInfoCodes.STATUS_01N81,
            "Server `%s` at address `%s` is still catching up."),

    WAIT_SERVER_FAILED(
            Status.Cluster.ServerFailed, GqlStatusInfoCodes.STATUS_01N80, "Server `%s` at address `%s` failed: %s"),

    WAIT_SERVER_CAUGHT_UP(
            Status.Cluster.ServerCaughtUp,
            GqlStatusInfoCodes.STATUS_03N85,
            "Server `%s` at address `%s` has caught up."),

    UNSUPPORTED_TYPE(
            Status.Request.UnsupportedType,
            GqlStatusInfoCodes.STATUS_01N83,
            "One or more values returned could not be handled by this version of the client "
                    + "and were replaced with placeholder map values. Please upgrade your client."),

    IDENTIFIER_SHADOWING_VARIABLE(
            Status.Statement.IdentifierShadowingVariable,
            GqlStatusInfoCodes.STATUS_03N63,
            "The identifier `%s` in the `%s` clause has the same name as a variable in scope. "
                    + "Regardless of what the variable evaluates to, it is the literal `%s` that will be used."),

    CALLABLE_SHADOWING(
            Status.Statement.CallableShadowing,
            GqlStatusInfoCodes.STATUS_03N64,
            "Local %s `%s` shadows a built-in or external %s with the same name.");

    private final Status status;
    private final GqlStatusInfoCodes gqlStatusInfo;
    private final int[] descriptionOffsets;
    private final String descriptionTemplate;
    private final String descriptionSubstitution;

    NotificationCodeWithDescription(Status status, GqlStatusInfoCodes gqlStatusInfo, String descriptionTemplate) {
        this.status = status;
        this.gqlStatusInfo = gqlStatusInfo;
        this.descriptionSubstitution = "%s";
        this.descriptionOffsets = gqlStatusInfo.getOffsets(descriptionTemplate, descriptionSubstitution);
        this.descriptionTemplate = descriptionTemplate;
    }

    public Status getStatus() {
        return status;
    }

    public GqlStatusInfo getGqlStatusInfo() {
        return gqlStatusInfo;
    }

    public String getDescription(Object[] args) {
        return SimpleMessageFormatter.format(descriptionTemplate, descriptionSubstitution, descriptionOffsets, args);
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
            InputPosition position, String oldDetail, List<String> variableNames) {
        return JOIN_HINT_UNFULFILLABLE.notificationWithParameters(
                position, new String[] {oldDetail}, new List[] {variableNames});
    }

    public static NotificationImplementation indexLookupForDynamicProperty(
            InputPosition position, String oldDetails, List<String> parameters) {
        return INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notificationWithParameters(
                position, new String[] {oldDetails}, new List[] {parameters});
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
        return DEPRECATED_PROCEDURE_FIELD.notificationWithParameters(position, new String[] {param}, new String[] {
            String.format("`%s` returned by the procedure `%s` is deprecated.", field, procedure)
        });
    }

    public static NotificationImplementation deprecatedProcedureField(
            InputPosition position, String param, String procedure, String field) {
        return DEPRECATED_PROCEDURE_FIELD.notificationWithParameters(position, new String[] {param}, new String[] {
            String.format("`%s` used by the procedure `%s` is deprecated.", field, procedure)
        });
    }

    public static NotificationImplementation deprecatedFunctionField(
            InputPosition position, String param, String function, String field) {
        return DEPRECATED_FUNCTION_FIELD.notificationWithParameters(position, new String[] {param}, new String[] {
            String.format("`%s` used by the function `%s` is deprecated.", field, function)
        });
    }

    public static NotificationImplementation deprecatedFunctionNamespace(InputPosition position, String callable) {
        return DEPRECATED_FUNCTION_NAMESPACE.notificationWithParameters(
                position, new String[] {callable}, new String[] {
                    String.format("The namespace used by the user-defined function `%s` is deprecated.", callable)
                });
    }

    public static NotificationImplementation deprecatedProcedureNamespace(InputPosition position, String callable) {
        return DEPRECATED_PROCEDURE_NAMESPACE.notificationWithParameters(
                position, new String[] {callable}, new String[] {
                    String.format("The namespace used by the user-defined procedure `%s` is deprecated.", callable)
                });
    }

    public static NotificationImplementation shadowingInternalFunction(InputPosition position, String callable) {
        return SHADOWING_INTERNAL_FUNCTION.notificationWithParameters(position, new String[] {callable}, new String[] {
            String.format(
                    "The namespace of the invoked user-defined function `%s` is deprecated and the function is shadowing an internal function.",
                    callable)
        });
    }

    public static NotificationImplementation deprecatedRelationshipTypeSeparator(
            InputPosition position, String param, String deprecated, String replacement) {
        var formattedDeprecated = GqlParams.StringParam.cmd.process(deprecated);
        var formattedReplacement = GqlParams.StringParam.cmd.process(replacement);
        return DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notificationWithParameters(
                position, new String[] {param}, new String[] {formattedDeprecated, formattedReplacement});
    }

    public static NotificationImplementation deprecatedNodeOrRelationshipOnRhsSetClause(
            InputPosition position, String deprecated, String replacement) {
        var formattedDeprecated = GqlParams.StringParam.cmd.process(deprecated);
        var formattedReplacement = GqlParams.StringParam.cmd.process(replacement);
        return DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE.notificationWithParameters(
                position, new String[] {}, new String[] {formattedDeprecated, formattedReplacement});
    }

    public static NotificationImplementation deprecatedPropertyReferenceInCreate(InputPosition position, String param) {
        return DEPRECATED_PROPERTY_REFERENCE_IN_CREATE.notificationWithParameters(
                position, new String[] {param}, new String[] {
                    String.format(
                            "Creating an entity (%s) and referencing that entity in a property definition in the same CREATE is deprecated.",
                            param)
                });
    }

    public static NotificationImplementation deprecatedPropertyReferenceInMerge(InputPosition position, String param) {
        return DEPRECATED_PROPERTY_REFERENCE_IN_MERGE.notificationWithParameters(
                position, new String[] {param}, new String[] {
                    String.format(
                            "Merging an entity (%s) and referencing that entity in a property definition in the same MERGE is deprecated.",
                            param)
                });
    }

    public static NotificationImplementation deprecatedShortestPathWithFixedLengthRelationship(
            InputPosition position, String deprecated, String replacement) {
        return DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP.notificationWithParameters(
                position, new String[] {}, new String[] {deprecated, replacement});
    }

    public static NotificationImplementation deprecatedTextIndexProvider(InputPosition position) {
        return DEPRECATED_TEXT_INDEX_PROVIDER.notificationWithParameters(position, new String[] {}, new String[] {
            AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR.name(), AllIndexProviderDescriptors.TEXT_V2_DESCRIPTOR.name()
        });
    }

    public static NotificationImplementation deprecatedIndexProviderOption(InputPosition position) {
        return DEPRECATED_INDEX_PROVIDER_OPTION.notificationWithParameters(position, new String[] {}, new String[] {
            "The `indexProvider` option is deprecated and will be removed in a future version. Neo4j does not use the given option but instead selects the most performant index provider available."
        });
    }

    public static NotificationImplementation deprecatedIdentifierWhitespaceUnicode(
            InputPosition position, Character unicode, String identifier) {
        String formattedUnicode = String.format("\\u%04x", (int) unicode);
        return DEPRECATED_IDENTIFIER_WHITESPACE_UNICODE.notificationWithParameters(
                position, new String[] {formattedUnicode, identifier}, new String[] {
                    String.format(
                            "The Unicode character `%s` is deprecated for unescaped identifiers and will be considered as a whitespace character in the future. "
                                    + "To continue using it, escape the identifier by adding backticks around the identifier `%s`.",
                            formattedUnicode, identifier)
                });
    }

    public static NotificationImplementation deprecatedIdentifierUnicode(
            InputPosition position, Character unicode, String identifier) {
        String formattedUnicode = String.format("\\u%04x", (int) unicode);
        return DEPRECATED_IDENTIFIER_UNICODE.notificationWithParameters(
                position, new String[] {formattedUnicode, identifier}, new String[] {
                    String.format(
                            "The character with the Unicode representation `%s` is deprecated for unescaped identifiers and will not be supported in the future. "
                                    + "To continue using it, escape the identifier by adding backticks around the identifier `%s`.",
                            formattedUnicode, identifier)
                });
    }

    public static NotificationImplementation eagerLoadCsv(InputPosition position) {
        return EAGER_LOAD_CSV.notification(position);
    }

    public static NotificationImplementation graphEngineFallbackPostProcessing() {
        return VIRTUAL_GRAPH_POST_PROCESSING.notification(InputPosition.empty);
    }

    public static NotificationImplementation deprecatedFormat(
            InputPosition position, String oldDetail, String deprecatedFormat, String newFormat) {
        return DEPRECATED_FORMAT.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {deprecatedFormat, newFormat});
    }

    public static NotificationImplementation deprecatedRequestedFeature(
            InputPosition position, String deprecated, String replacement) {
        return DEPRECATED_REQUESTED_FEATURE.notificationWithParameters(
                position, new String[] {deprecated, replacement}, new String[] {deprecated, replacement});
    }

    public static NotificationImplementation largeLabelLoadCsv(InputPosition position, String labelName) {
        return LARGE_LABEL_LOAD_CSV.notificationWithParameters(position, new String[] {}, new String[] {labelName});
    }

    public static NotificationImplementation missingLabel(
            InputPosition position, String oldDetail, String labelName, String db) {
        return MISSING_LABEL.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {labelName, db});
    }

    public static NotificationImplementation missingRelType(
            InputPosition position, String oldDetail, String relType, String db) {
        return MISSING_REL_TYPE.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {relType, db});
    }

    public static NotificationImplementation missingPropertyName(
            InputPosition position, String oldDetails, String property, String db) {
        return MISSING_PROPERTY_NAME.notificationWithParameters(
                position, new String[] {oldDetails}, new String[] {property, db});
    }

    public static NotificationImplementation unboundedShortestPath(InputPosition position, String pattern) {
        return UNBOUNDED_SHORTEST_PATH.notificationWithParameters(position, new String[] {}, new String[] {pattern});
    }

    public static NotificationImplementation exhaustiveShortestPath(
            InputPosition position, List<String> pathPredicates) {
        return EXHAUSTIVE_SHORTEST_PATH.notificationWithParameters(
                position, new String[] {}, new List[] {pathPredicates});
    }

    public static NotificationImplementation missingParameterForExplain(
            InputPosition position, String oldDetails, List<String> parameters) {
        return MISSING_PARAMETERS_FOR_EXPLAIN.notificationWithParameters(
                position, new String[] {oldDetails}, new List[] {parameters});
    }

    public static NotificationImplementation codeGenerationFailed(
            InputPosition position, String failingRuntimeConf, String fallbackRuntimeConf, String cause) {
        final var oldDetails = new String[] {cause};
        final boolean operatorFellBack = fallbackRuntimeConf.contains("operatorEngine=interpreted")
                && failingRuntimeConf.contains("operatorEngine=");
        final boolean expressionFellBack = fallbackRuntimeConf.contains("expressionEngine=interpreted")
                && failingRuntimeConf.contains("expressionEngine=");
        final String failingEngine;
        if (operatorFellBack && expressionFellBack) {
            failingEngine = "operator and expression";
        } else if (operatorFellBack) {
            failingEngine = "operator";
        } else if (expressionFellBack) {
            failingEngine = "expression";
        } else {
            failingEngine = ""; // should not happen
        }
        final Object[] params = {failingEngine, cause};
        return CODE_GENERATION_FAILED.notificationWithParameters(position, oldDetails, params);
    }

    public static NotificationImplementation subqueryVariableShadowing(
            InputPosition position, String oldDetail, String subqueryType, String variable) {
        return SUBQUERY_VARIABLE_SHADOWING.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {variable, subqueryType, variable});
    }

    public static NotificationImplementation redundantOptionalProcedure(InputPosition position, String proc) {
        return REDUNDANT_OPTIONAL_PROCEDURE.notificationWithParameters(
                position, new String[] {proc}, new String[] {proc});
    }

    public static NotificationImplementation redundantOptionalSubquery(InputPosition position) {
        return REDUNDANT_OPTIONAL_SUBQUERY.notificationWithParameters(position, new String[] {}, new String[] {});
    }

    public static NotificationImplementation deprecatedComplexSubclauseExpression(
            InputPosition position, String subclauseExpr, String groupingExpr) {
        return DEPRECATED_COMPLEX_SUBCLAUSE_EXPRESSION.notificationWithParameters(
                position, new String[] {subclauseExpr, groupingExpr}, new String[] {
                    String.format(
                            "The subclause expression `%s` contains a reference to the complex projection item expression `%s` and",
                            subclauseExpr, groupingExpr),
                    "referencing the projection item expression by an alias"
                });
    }

    public static NotificationImplementation deprecatedAmbiguousSubclauseExpression(
            InputPosition position, String subclauseExpr, String variable) {
        return DEPRECATED_AMBIGUOUS_SUBCLAUSE_EXPRESSION.notificationWithParameters(
                position, new String[] {subclauseExpr, variable}, new String[] {
                    String.format(
                            "The subclause expression `%s` contains an ambiguous reference to variable `%s` and",
                            subclauseExpr, variable),
                    "referencing the projection item expression by an alias or using the `GROUP BY` clause"
                });
    }

    public static NotificationImplementation deprecatedAmbiguousAndComplexSubclauseExpression(
            InputPosition position, String subclauseExpr, String variable, String groupingExpr) {
        return DEPRECATED_AMBIGUOUS_AND_COMPLEX_SUBCLAUSE_EXPRESSION.notificationWithParameters(
                position, new String[] {subclauseExpr, variable, groupingExpr}, new String[] {
                    String.format(
                            "The subclause expression `%s` contains an ambiguous reference to variable `%s` and a reference to the complex projection item expression `%s` and",
                            subclauseExpr, variable, groupingExpr),
                    "referencing the projection item expression by an alias or using the `GROUP BY` clause"
                });
    }

    public static NotificationImplementation deprecatedImportingWithInSubqueryCall(
            InputPosition position, String subqueryType, String variable) {
        return DEPRECATED_IMPORTING_WITH_IN_SUBQUERY_CALL.notificationWithParameters(
                position, new String[] {subqueryType, subqueryType, variable}, new String[] {
                    String.format(
                            "%s subquery without a variable scope clause is deprecated. Use %s (%s) { ... }",
                            subqueryType, subqueryType, variable)
                });
    }

    public static NotificationImplementation deprecatedWhereVariableInNodePattern(
            InputPosition position, String variableName, String properties) {
        String nodePatternWithUnescapedVariable = String.format("(%s %s)", variableName, properties);
        String nodePatternWithEscapedVariable = String.format("(`%s` %s)", variableName, properties);
        return DEPRECATED_WHERE_VARIABLE_IN_NODE_PATTERN.notificationWithParameters(
                position,
                new String[] {nodePatternWithUnescapedVariable, nodePatternWithEscapedVariable},
                new String[] {nodePatternWithUnescapedVariable, nodePatternWithEscapedVariable});
    }

    public static NotificationImplementation deprecatedWhereVariableInRelationshipPattern(
            InputPosition position, String variableName, String properties) {
        String relPatternWithUnescapedVariable = String.format("-[%s %s]-", variableName, properties);
        String relPatternWithEscapedVariable = String.format("-[`%s` %s]-", variableName, properties);
        return DEPRECATED_WHERE_VARIABLE_IN_RELATIONSHIP_PATTERN.notificationWithParameters(
                position,
                new String[] {relPatternWithUnescapedVariable, relPatternWithEscapedVariable},
                new String[] {relPatternWithUnescapedVariable, relPatternWithEscapedVariable});
    }

    public static NotificationImplementation deprecatedPrecedenceOfLabelExpressionPredicate(
            InputPosition position, String labelExpressionPredicate) {
        String unparenthesizedLabelExpressionPredicate = String.format("... + %s", labelExpressionPredicate);
        String parenthesizedLabelExpressionPredicate = String.format("... + (%s)", labelExpressionPredicate);
        return DEPRECATED_PRECEDENCE_OF_LABEL_EXPRESSION_PREDICATED.notificationWithParameters(
                position,
                new String[] {unparenthesizedLabelExpressionPredicate, parenthesizedLabelExpressionPredicate},
                new String[] {unparenthesizedLabelExpressionPredicate, parenthesizedLabelExpressionPredicate});
    }

    public static NotificationImplementation deprecatedKeywordVariableInWhenOperand(
            InputPosition position, String variableName, String remainingExpression) {
        String whenOperandWithUnescapedVariable = String.format("WHEN %s%s", variableName, remainingExpression);
        String whenOperandWithEscapedVariable = String.format("WHEN `%s`%s", variableName, remainingExpression);
        return DEPRECATED_KEYWORD_VARIABLE_IN_WHEN_OPERAND.notificationWithParameters(
                position,
                new String[] {whenOperandWithUnescapedVariable, whenOperandWithEscapedVariable},
                new String[] {whenOperandWithUnescapedVariable, whenOperandWithEscapedVariable});
    }

    public static NotificationImplementation homeDatabaseNotPresent(
            InputPosition position, String oldDetail, String missingDb) {
        return HOME_DATABASE_NOT_PRESENT.notificationWithParameters(
                position, new String[] {oldDetail}, new String[] {missingDb});
    }

    public static NotificationImplementation deprecatedGraphReferenceNotification(
            String quotedGraphName, String futureGraphName, InputPosition position) {
        return DEPRECATED_QUOTED_GRAPH_REFERENCE.notificationWithParameters(
                position, new String[] {quotedGraphName, futureGraphName}, new String[] {
                    DEPRECATED_QUOTED_GRAPH_REFERENCE.descriptionTemplate.formatted(quotedGraphName, futureGraphName),
                });
    }

    public static NotificationImplementation deprecatedQuotedGraphByNameArgument(
            InputPosition position, String quotedGraphName, String futureGraphName) {
        return DEPRECATED_QUOTED_GRAPH_BY_NAME_ARGUMENT.notificationWithParameters(
                position, new String[] {quotedGraphName, futureGraphName}, new String[] {
                    DEPRECATED_QUOTED_GRAPH_BY_NAME_ARGUMENT.descriptionTemplate.formatted(
                            quotedGraphName, futureGraphName),
                });
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

    public static NotificationImplementation deprecatedEagerAnalyzerPreParserOption(InputPosition position) {
        return DEPRECATED_EAGER_ANALYZER_PRE_PARSER_OPTION.notificationWithParameters(
                position, new String[] {}, new String[] {"eagerAnalyzer"});
    }

    public static NotificationImplementation retiredPlannerVersionPreParserOption(
            InputPosition position, String version) {
        return RETIRED_PLANNER_VERSION_PRE_PARSER_OPTION.notificationWithParameters(
                position, new String[] {version}, new String[] {version});
    }

    public static NotificationImplementation authProviderNotDefined(InputPosition position, String provider) {
        return AUTH_PROVIDER_NOT_DEFINED.notificationWithParameters(
                position, new String[] {provider, provider}, new String[] {provider, provider});
    }

    public static NotificationImplementation externalAuthNotEnabled(InputPosition position) {
        return EXTERNAL_AUTH_NOT_ENABLED.notification(position);
    }

    public static NotificationImplementation oidcCredentialForwardingNotEnabled(InputPosition position) {
        return OIDC_CREDENTIAL_FORWARDING_NOT_ENABLED.notification(position);
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

    public static NotificationImplementation commandHasNoEffectGrantRoleToAuthRule(
            InputPosition position, String titleParam) {
        return COMMAND_HAS_NO_EFFECT_GRANT_ROLE_TO_AUTH_RULE.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {}, new String[] {titleParam});
    }

    public static NotificationImplementation commandHasNoEffectRevokeRoleToAuthRule(
            InputPosition position, String titleParam) {
        return COMMAND_HAS_NO_EFFECT_REVOKE_ROLE_TO_AUTH_RULE.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {}, new String[] {titleParam});
    }

    public static NotificationImplementation impossibleRevokeCommand(
            InputPosition position, String titleParam, String descriptionParam) {
        return IMPOSSIBLE_REVOKE_COMMAND.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {descriptionParam}, new String[] {titleParam, descriptionParam});
    }

    public static NotificationImplementation shardedPerformance() {
        return SHARDED_PRIVILEGE_PERFORMANCE.notification(InputPosition.empty);
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

    public static NotificationImplementation cordonedServersExist(InputPosition position, List<String> servers) {
        // Keep description without spaces to avoid breaking change
        String serverStringWithoutSpaces = String.join(",", servers);

        return CORDONED_SERVERS_EXISTED_DURING_ALLOCATION.notificationWithParameters(
                position, new String[] {serverStringWithoutSpaces}, new List[] {servers});
    }

    public static NotificationImplementation requestedTopologyMatchedCurrentTopology(InputPosition position) {
        return REQUESTED_TOPOLOGY_MATCHED_CURRENT_TOPOLOGY.notification(position);
    }

    public static NotificationImplementation aggregationSkippedNull() {
        return AGGREGATION_SKIPPED_NULL.notification(InputPosition.empty);
    }

    public static NotificationImplementation deprecatedBooleanCoercion() {
        return DEPRECATED_BOOLEAN_COERCION.notificationWithParameters(
                InputPosition.empty, new String[] {}, new String[] {"Converting a list or a path to a boolean"});
    }

    public static NotificationImplementation insecureProtocol() {
        return INSECURE_PROTOCOL.notification(InputPosition.empty);
    }

    public static NotificationImplementation indexOrConstraintAlreadyExists(
            InputPosition position, String titleParam, String descriptionParam) {
        return INDEX_OR_CONSTRAINT_ALREADY_EXISTS.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {descriptionParam}, new String[] {titleParam, descriptionParam});
    }

    public static NotificationImplementation indexOrConstraintDoesNotExist(
            InputPosition position, String titleParam, String descriptionParam) {
        return INDEX_OR_CONSTRAINT_DOES_NOT_EXIST.notificationWithTitleAndDescriptionDetails(
                position, titleParam, new String[] {descriptionParam}, new String[] {titleParam, descriptionParam});
    }

    public static NotificationImplementation vectorIndexDimensionsNotSpecified(InputPosition position) {
        return VECTOR_INDEX_DIMENSIONS_NOT_SPECIFIED.notification(position);
    }

    public static NotificationImplementation deprecatedOptionInOptionMap(String oldOption, String newOption) {
        return DEPRECATED_OPTION_IN_OPTION_MAP.notificationWithParameters(
                InputPosition.empty, new String[] {oldOption, newOption}, new String[] {oldOption, newOption});
    }

    public static NotificationImplementation deprecatedSeedingOption(String oldOption) {
        return DEPRECATED_SEEDING_OPTION.notificationWithParameters(
                InputPosition.empty, new String[] {oldOption}, new String[] {oldOption});
    }

    public static NotificationImplementation deprecatedExistingDataOption() {
        return DEPRECATED_EXISTING_DATA_OPTION.notificationWithParameters(
                InputPosition.empty, new String[] {"existingData"}, new String[] {"existingData"});
    }

    public static NotificationImplementation deprecatedStoreFormat(String format) {
        return DEPRECATED_STORE_FORMAT.notificationWithParameters(
                InputPosition.empty,
                new String[] {format},
                new String[] {DeprecatedFormatWarning.getTargetFormatWarning(format)});
    }

    public static NotificationImplementation waitServerUnavailable(String serverName) {
        return WAIT_SERVER_UNAVAILABLE.notificationWithParameters(
                InputPosition.empty, new String[] {serverName}, new String[] {serverName});
    }

    public static NotificationImplementation waitServerCatchingUp(String serverName, String boltAddress) {
        return WAIT_SERVER_CATCHING_UP.notificationWithParameters(
                InputPosition.empty, new String[] {serverName, boltAddress}, new String[] {serverName, boltAddress});
    }

    public static NotificationImplementation waitServerFailed(String serverName, String boltAddress, String message) {
        return WAIT_SERVER_FAILED.notificationWithParameters(
                InputPosition.empty,
                new String[] {serverName, boltAddress, message},
                new String[] {serverName, boltAddress, message});
    }

    public static NotificationImplementation clientDoesNotSupportType(String type) {
        return UNSUPPORTED_TYPE.notificationWithParameters(
                InputPosition.empty, new String[] {type}, new String[] {type});
    }

    public static NotificationImplementation identifierShadowingVariable(
            InputPosition position, String identifier, String clause) {
        return IDENTIFIER_SHADOWING_VARIABLE.notificationWithParameters(
                position, new String[] {identifier, clause, identifier}, new String[] {identifier, clause, identifier});
    }

    public static NotificationImplementation waitServerCaughtUp(String serverName, String boltAddress) {
        return WAIT_SERVER_CAUGHT_UP.notificationWithParameters(
                InputPosition.empty, new String[] {serverName, boltAddress}, new String[] {serverName, boltAddress});
    }

    public static NotificationImplementation callableShadowing(
            InputPosition position, String callableKind, String callableName) {
        return CALLABLE_SHADOWING.notificationWithParameters(
                position,
                new String[] {callableKind, callableName, callableKind},
                new String[] {callableKind, callableName, callableKind});
    }

    private NotificationImplementation notification(InputPosition position) {
        return notificationWithParameters(position, new String[] {}, new String[] {});
    }

    private NotificationImplementation notificationWithParameters(
            InputPosition position, String[] oldDetails, Object[] parameterValues) {
        return new NotificationImplementation.NotificationBuilder(this)
                .setPosition(position)
                .setNotificationDetails(oldDetails)
                .setMessageParameters(parameterValues)
                .build();
    }

    private NotificationImplementation notificationWithTitleAndDescriptionDetails(
            InputPosition position, String titleDetail, String[] descriptionDetails, Object[] parameterValues) {
        // Allows a single detail in the title and multiple in the description
        return new NotificationImplementation.NotificationBuilder(this)
                .setPosition(position)
                .setTitleDetails(titleDetail)
                .setNotificationDetails(descriptionDetails)
                .setMessageParameters(parameterValues)
                .build();
    }
}
