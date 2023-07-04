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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.codeGenerationFailed;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.commandHasNoEffect;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedDatabaseName;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedFormat;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedFunction;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedNodeOrRelationshipOnRhsSetClause;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedProcedure;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedProcedureReturnField;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedPropertyReferenceInCreate;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedRelationshipTypeSeparator;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedRuntimeOption;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.deprecatedShortestPathWithFixedLengthRelationship;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.eagerLoadCsv;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.exhaustiveShortestPath;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.homeDatabaseNotPresent;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.impossibleRevokeCommand;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.indexHintUnfulfillable;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.indexLookupForDynamicProperty;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.joinHintUnfulfillable;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.largeLabelLoadCsv;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.missingLabel;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.missingParameterForExplain;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.missingPropertyName;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.missingRelType;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.procedureWarning;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.repeatedRelationshipReference;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.repeatedVarLengthRelationshipReference;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.runtimeExperimental;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.runtimeUnsupported;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.subqueryVariableShadowing;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.unboundedShortestPath;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.unionReturnOrder;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.unsatisfiableRelationshipTypeExpression;
import static org.neo4j.graphdb.impl.notification.NotificationDetail.repeatedRelationship;
import static org.neo4j.graphdb.impl.notification.NotificationDetail.unsatisfiableRelTypeExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;

class NotificationCodeWithDescriptionTest {
    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_node_index() {
        String indexDetail = NotificationDetail.nodeAnyIndex("person", "Person", "name");
        NotificationImplementation notification = indexHintUnfulfillable(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: INDEX FOR (`person`:`Person`) ON (`person`.`name`))",
                NotificationCategory.HINT,
                null);
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_node_text_index() {
        String indexDetail = NotificationDetail.nodeTextIndex("person", "Person", "name");
        NotificationImplementation notification = indexHintUnfulfillable(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: TEXT INDEX FOR (`person`:`Person`) ON (`person`.`name`))",
                NotificationCategory.HINT,
                null);
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_rel_index() {
        String indexDetail = NotificationDetail.relationshipAnyIndex("person", "Person", "name");
        NotificationImplementation notification = indexHintUnfulfillable(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`))",
                NotificationCategory.HINT,
                null);
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_rel_text_index() {
        String indexDetail = NotificationDetail.relationshipTextIndex("person", "Person", "name");
        NotificationImplementation notification = indexHintUnfulfillable(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: TEXT INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`))",
                NotificationCategory.HINT,
                null);
    }

    @Test
    void shouldConstructNotificationFor_CARTESIAN_PRODUCT() {
        Set<String> idents = new TreeSet<>();
        idents.add("n");
        idents.add("node2");
        String identifierDetail = NotificationDetail.cartesianProductDescription(idents);
        NotificationImplementation notification =
                NotificationCodeWithDescription.cartesianProduct(InputPosition.empty, identifierDetail);

        verifyNotification(
                notification,
                "This query builds a cartesian product between disconnected patterns.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.CartesianProduct",
                "If a part of a query contains multiple disconnected patterns, this will build a cartesian product "
                        + "between all those parts. This may produce a large amount of data and slow down query processing. While "
                        + "occasionally intended, it may often be possible to reformulate the query that avoids the use of this cross "
                        + "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH "
                        + "(identifiers are: (n, node2))",
                NotificationCategory.PERFORMANCE,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_JOIN_HINT_UNFULFILLABLE() {
        List<String> idents = new ArrayList<>();
        idents.add("n");
        idents.add("node2");
        String identifierDetail = NotificationDetail.joinKey(idents);
        NotificationImplementation notification = joinHintUnfulfillable(InputPosition.empty, identifierDetail);

        verifyNotification(
                notification,
                "The database was unable to plan a hinted join.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.JoinHintUnfulfillableWarning",
                "The hinted join was not planned. This could happen because no generated plan contained the join key, "
                        + "please try using a different join key or restructure your query. "
                        + "(hinted join key identifiers are: n, node2)",
                NotificationCategory.HINT,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE() {
        String identifierDetail = NotificationDetail.deprecatedName("oldName", "newName");
        NotificationImplementation notification = deprecatedProcedure(InputPosition.empty, identifierDetail);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated procedure. ('oldName' has been replaced by 'newName')",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE_with_no_newName() {
        String identifierDetail = NotificationDetail.deprecatedName("oldName", "");
        NotificationImplementation notification = deprecatedProcedure(InputPosition.empty, identifierDetail);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated procedure: `oldName`.",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_RUNTIME_UNSUPPORTED() {
        NotificationImplementation notification = runtimeUnsupported(InputPosition.empty, "PARALLEL");

        verifyNotification(
                notification,
                "This query is not supported by the chosen runtime.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RuntimeUnsupportedWarning",
                "Selected runtime is unsupported for this query, please use a different runtime instead or fallback to default. (PARALLEL)",
                NotificationCategory.UNSUPPORTED,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY() {
        NotificationImplementation notification = indexLookupForDynamicProperty(InputPosition.empty, "m[n.x]");

        verifyNotification(
                notification,
                "Queries using dynamic properties will use neither index seeks nor index scans for those properties",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.DynamicProperty",
                "Using a dynamic property makes it impossible to use an index lookup for this query (m[n.x])",
                NotificationCategory.PERFORMANCE,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_FUNCTION() {
        String identifierDetail = NotificationDetail.deprecatedName("oldName", "newName");
        NotificationImplementation notification = deprecatedFunction(InputPosition.empty, identifierDetail);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated function. ('oldName' has been replaced by 'newName')",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_FUNCTION_with_no_newName() {
        String identifierDetail = NotificationDetail.deprecatedName("oldName", "");
        NotificationImplementation notification = deprecatedFunction(InputPosition.empty, identifierDetail);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated function: `oldName`.",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_RUNTIME_OPTION() {
        NotificationImplementation notification =
                deprecatedRuntimeOption(InputPosition.empty, "option=deprecatedOption");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated runtime option. (option=deprecatedOption)",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_PROCEDURE_WARNING() {
        NotificationImplementation notification = procedureWarning(InputPosition.empty, "deprecated procedure");

        verifyNotification(
                notification,
                "The query used a procedure that generated a warning.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Procedure.ProcedureWarning",
                "The query used a procedure that generated a warning. (deprecated procedure)",
                NotificationCategory.GENERIC,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE_RETURN_FIELD() {
        NotificationImplementation notification =
                deprecatedProcedureReturnField(InputPosition.empty, "deprecatedField");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated field from a procedure. (deprecatedField)",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR() {
        NotificationImplementation notification = deprecatedRelationshipTypeSeparator(InputPosition.empty, "a:b");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The semantics of using colon in the separation of alternative relationship types will change in a future version. (a:b)",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE() {
        NotificationImplementation notification = deprecatedNodeOrRelationshipOnRhsSetClause(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The use of nodes or relationships for setting properties is deprecated and will be removed in a future version. "
                        + "Please use properties() instead.",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP() {
        NotificationImplementation notification =
                deprecatedShortestPathWithFixedLengthRelationship(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The use of shortestPath and allShortestPaths with fixed length relationships is deprecated and will be removed in a future version. "
                        + "Please use a path with a length of 1 [r*1..1] instead or a Match with a limit.",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_EAGER_LOAD_CSV() {
        NotificationImplementation notification = eagerLoadCsv(InputPosition.empty);

        verifyNotification(
                notification,
                "The execution plan for this query contains the Eager operator, "
                        + "which forces all dependent data to be materialized in main memory before proceeding",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.EagerOperator",
                "Using LOAD CSV with a large data set in a query where the execution plan contains the "
                        + "Eager operator could potentially consume a lot of memory and is likely to not perform well. "
                        + "See the Neo4j Manual entry on the Eager operator for more information and hints on "
                        + "how problems could be avoided.",
                NotificationCategory.PERFORMANCE,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_FORMAT() {
        NotificationImplementation notification =
                deprecatedFormat(InputPosition.empty, "u627", "deprecatedFormat", "newFormat");

        verifyNotification(
                notification,
                "The client made a request for a format which has been deprecated.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Request.DeprecatedFormat",
                "The requested format has been deprecated. (u627)",
                NotificationCategory.DEPRECATION,
                "The requested `deprecatedFormat` format is deprecated. Replace it with `newFormat`.");
    }

    @Test
    void shouldConstructNotificationsFor_LARGE_LABEL_LOAD_CSV() {
        NotificationImplementation notification = largeLabelLoadCsv(InputPosition.empty);

        verifyNotification(
                notification,
                "Adding a schema index may speed up this query.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.NoApplicableIndex",
                "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely "
                        + "not perform well on large data sets. Please consider using a schema index.",
                NotificationCategory.PERFORMANCE,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_LABEL() {
        NotificationImplementation notification = missingLabel(InputPosition.empty, "Label");

        verifyNotification(
                notification,
                "The provided label is not in the database.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownLabelWarning",
                "One of the labels in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application (Label)",
                NotificationCategory.UNRECOGNIZED,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_REL_TYPE() {
        NotificationImplementation notification = missingRelType(InputPosition.empty, "Rel");

        verifyNotification(
                notification,
                "The provided relationship type is not in the database.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownRelationshipTypeWarning",
                "One of the relationship types in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application (Rel)",
                NotificationCategory.UNRECOGNIZED,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PROPERTY_NAME() {
        NotificationImplementation notification = missingPropertyName(InputPosition.empty, "prop");

        verifyNotification(
                notification,
                "The provided property key is not in the database",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownPropertyKeyWarning",
                "One of the property names in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application (prop)",
                NotificationCategory.UNRECOGNIZED,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_UNBOUNDED_SHORTEST_PATH() {
        NotificationImplementation notification = unboundedShortestPath(InputPosition.empty);

        verifyNotification(
                notification,
                "The provided pattern is unbounded, consider adding an upper limit to the number of node hops.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.UnboundedVariableLengthPattern",
                "Using shortest path with an unbounded pattern will likely result in long execution times. "
                        + "It is recommended to use an upper limit to the number of node hops in your pattern.",
                NotificationCategory.PERFORMANCE,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_EXHAUSTIVE_SHORTEST_PATH() {
        NotificationImplementation notification = exhaustiveShortestPath(InputPosition.empty);

        verifyNotification(
                notification,
                "Exhaustive shortest path has been planned for your query that means that shortest path graph "
                        + "algorithm might not be used to find the shortest path. Hence an exhaustive enumeration of all paths "
                        + "might be used in order to find the requested shortest path.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.ExhaustiveShortestPath",
                "Using shortest path with an exhaustive search fallback might cause query slow down since shortest path "
                        + "graph algorithms might not work for this use case. It is recommended to introduce a WITH to separate the "
                        + "MATCH containing the shortest path from the existential predicates on that path.",
                NotificationCategory.PERFORMANCE,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_RUNTIME_EXPERIMENTAL() {
        NotificationImplementation notification = runtimeExperimental(InputPosition.empty, "PARALLEL");

        verifyNotification(
                notification,
                "This feature is experimental and should not be used in production systems.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RuntimeExperimental",
                "You are using an experimental feature (PARALLEL)",
                NotificationCategory.UNSUPPORTED,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PARAMETERS_FOR_EXPLAIN() {
        NotificationImplementation notification = missingParameterForExplain(InputPosition.empty, "param");

        verifyNotification(
                notification,
                "The statement refers to a parameter that was not provided in the request.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.ParameterNotProvided",
                "Did not supply query with enough parameters. "
                        + "The produced query plan will not be cached and is not executable without EXPLAIN. (param)",
                NotificationCategory.GENERIC,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_CODE_GENERATION_FAILED() {
        NotificationImplementation notification = codeGenerationFailed(InputPosition.empty, "method too big");

        verifyNotification(
                notification,
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.CodeGenerationFailed",
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log. (method too big)",
                NotificationCategory.PERFORMANCE,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_SUBQUERY_VARIABLE_SHADOWING() {
        NotificationImplementation notification = subqueryVariableShadowing(InputPosition.empty, "v");

        verifyNotification(
                notification,
                "Variable in subquery is shadowing a variable with the same name from the outer scope.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.SubqueryVariableShadowing",
                "Variable in subquery is shadowing a variable with the same name from the outer scope. "
                        + "If you want to use that variable instead, it must be imported into the subquery using importing WITH clause. (v)",
                NotificationCategory.GENERIC,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_HOME_DATABASE_NOT_PRESENT() {
        NotificationImplementation notification = homeDatabaseNotPresent(InputPosition.empty, "db");

        verifyNotification(
                notification,
                "The request referred to a home database that does not exist.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Database.HomeDatabaseNotFound",
                "The home database provided does not currently exist in the DBMS. "
                        + "This command will not take effect until this database is created. (db)",
                NotificationCategory.UNRECOGNIZED,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_DATABASE_NAME() {
        NotificationImplementation notification = deprecatedDatabaseName(InputPosition.empty, "db.one");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "Databases and aliases with unescaped `.` are deprecated unless to indicate that they belong to a composite database. "
                        + "Names containing `.` should be escaped. (db.one)",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION() {
        NotificationImplementation notification =
                unsatisfiableRelationshipTypeExpression(InputPosition.empty, unsatisfiableRelTypeExpression("!%"));

        verifyNotification(
                notification,
                "The query contains a relationship type expression that cannot be satisfied.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnsatisfiableRelationshipTypeExpression",
                "Relationship type expression cannot possibly be satisfied. (`!%` can never be satisfied by any relationship. Relationships must have exactly one relationship type.)",
                NotificationCategory.GENERIC,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_REPEATED_RELATIONSHIP_REFERENCE() {
        NotificationImplementation notification =
                repeatedRelationshipReference(InputPosition.empty, repeatedRelationship("r"));

        verifyNotification(
                notification,
                "The query returns no results due to repeated references to a relationship.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RepeatedRelationshipReference",
                "A relationship is referenced more than once in the query, which leads to no results because relationships must not occur more than once in each result. (Relationship `r` was repeated)",
                NotificationCategory.GENERIC,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE() {
        NotificationImplementation notification =
                repeatedVarLengthRelationshipReference(InputPosition.empty, repeatedRelationship("r"));

        verifyNotification(
                notification,
                "The query returns no results due to repeated references to a relationship.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RepeatedRelationshipReference",
                "A variable-length relationship variable is bound more than once, which leads to no results because relationships must not occur more than once in each result. (Relationship `r` was repeated)",
                NotificationCategory.GENERIC,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_UNION_RETURN_ORDER() {
        NotificationImplementation notification = unionReturnOrder(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "All subqueries in a UNION [ALL] should have the same ordering for the return columns. Using differently ordered return items in a UNION [ALL] clause is deprecated and will be removed in a future version.",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_CONNECT_COMPONENTS_PLANNER_PRE_PARSER_OPTION() {
        NotificationImplementation notification =
                deprecatedConnectComponentsPlannerPreParserOption(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The Cypher query option `connectComponentsPlanner` is deprecated and will be removed without a replacement. "
                        + "The product's default behavior of using a cost-based IDP search algorithm when combining sub-plans will be kept. "
                        + "For more information, see Cypher Manual -> Cypher planner.",
                NotificationCategory.DEPRECATION,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_COMMAND_HAD_NO_EFFECT() {
        NotificationImplementation notification = commandHasNoEffect(
                InputPosition.empty, "CREATE DATABASE db IF NOT EXISTS", "Database db already exist.");

        verifyNotification(
                notification,
                "`CREATE DATABASE db IF NOT EXISTS` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Security.CommandHasNoEffect",
                "Database db already exist. See Status Codes documentation for more information.",
                NotificationCategory.SECURITY,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_IMPOSSIBLE_REVOKE_COMMAND() {
        NotificationImplementation notification =
                impossibleRevokeCommand(InputPosition.empty, "REVOKE admin FROM ALICE", "Role does not exist.");

        verifyNotification(
                notification,
                "`REVOKE admin FROM ALICE` has no effect.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Security.ImpossibleRevokeCommand",
                "Role does not exist. Make sure nothing is misspelled. This notification will become an error in a future major version. "
                        + "See Status Codes documentation for more information.",
                NotificationCategory.SECURITY,
                null);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROPERTY_REFERENCE_IN_CREATE() {
        NotificationImplementation notification = deprecatedPropertyReferenceInCreate(InputPosition.empty, "n.prop");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "Creating an entity (n.prop) and referencing that entity in a property definition in the same CREATE is deprecated.",
                NotificationCategory.DEPRECATION,
                null);
    }

    private void verifyNotification(
            NotificationImplementation notification,
            String title,
            SeverityLevel severity,
            String code,
            String description,
            NotificationCategory category,
            String message) {
        assertThat(notification.getTitle()).isEqualTo(title);
        assertThat(notification.getSeverity()).isEqualTo(severity);
        assertThat(notification.getCode()).isEqualTo(code);
        assertThat(notification.getDescription()).isEqualTo(description);
        assertThat(notification.getCategory()).isEqualTo(category);
        assertThat(notification.getMessage()).isEqualTo(message);
    }

    @Test
    void allNotificationsShouldBeAClientNotification() {

        Arrays.stream(NotificationCodeWithDescription.values())
                .forEach(notification ->
                        assertThat(notification.getStatus().code().serialize()).contains("ClientNotification"));
    }

    @Test
    void noNotificationShouldHaveUnknownCategory() {
        Arrays.stream(NotificationCodeWithDescription.values()).forEach(notification -> assertThat(
                        ((Status.NotificationCode) notification.getStatus().code()).getNotificationCategory())
                .isNotEqualTo(NotificationCategory.UNKNOWN.name()));
    }

    /**
     * If this test fails, you have added, changed or removed a notification.
     * To get it approved, follow the instructions on
     * https://trello.com/c/9L3lbeSY/27-update-to-notification-name
     * When your changes have been approved, please change the expected byte[] below.
     */
    @Test
    void verifyNotificationsHaveNotChanged() {
        StringBuilder notificationBuilder = new StringBuilder();
        Arrays.stream(NotificationCodeWithDescription.values()).forEach(notification -> {
            var status = notification.getStatus();
            Status.NotificationCode notificationCode = (Status.NotificationCode) status.code();

            // Covers all notification information except NotificationDetail and position, which are query dependent
            notificationBuilder.append(notificationCode.description()); // Title
            notificationBuilder.append(notification.getDescription()); // Description
            notificationBuilder.append(notificationCode.serialize());
            notificationBuilder.append(notificationCode.getSeverity());
            notificationBuilder.append(notificationCode.getNotificationCategory());
        });

        byte[] notificationHash = DigestUtils.sha256(notificationBuilder.toString());

        byte[] expectedHash = new byte[] {
            -39, -7, -62, 83, -119, 77, -27, 35, 17, -13, 112, -36, 61, 27, -17, -40, -93, 37, 30, -72, -4, -14, 28,
            -85, -10, -126, 18, 103, -88, 67, 1, 49
        };

        if (!Arrays.equals(notificationHash, expectedHash)) {
            fail("Expected: " + Arrays.toString(expectedHash) + " \n Actual: " + Arrays.toString(notificationHash)
                    + "\n If you have added, changed or removed a notification, "
                    + "please follow the process on https://trello.com/c/9L3lbeSY/27-update-to-notification-name");
        }
    }
}
