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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.notifications.NotificationCodeWithDescription.cartesianProduct;
import static org.neo4j.notifications.NotificationCodeWithDescription.codeGenerationFailed;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectAssignPrivilege;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectGrantRole;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectRevokePrivilege;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectRevokeRole;
import static org.neo4j.notifications.NotificationCodeWithDescription.cordonedServersExist;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedDatabaseName;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFormat;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionWithReplacement;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionWithoutReplacement;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedIdentifierUnicode;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedIdentifierWhitespaceUnicode;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedNodeOrRelationshipOnRhsSetClause;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureReturnField;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureWithReplacement;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureWithoutReplacement;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedPropertyReferenceInCreate;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedRelationshipTypeSeparator;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedRuntimeOption;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedShortestPathWithFixedLengthRelationship;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedTextIndexProvider;
import static org.neo4j.notifications.NotificationCodeWithDescription.eagerLoadCsv;
import static org.neo4j.notifications.NotificationCodeWithDescription.exhaustiveShortestPath;
import static org.neo4j.notifications.NotificationCodeWithDescription.homeDatabaseNotPresent;
import static org.neo4j.notifications.NotificationCodeWithDescription.impossibleRevokeCommand;
import static org.neo4j.notifications.NotificationCodeWithDescription.indexHintUnfulfillable;
import static org.neo4j.notifications.NotificationCodeWithDescription.indexLookupForDynamicProperty;
import static org.neo4j.notifications.NotificationCodeWithDescription.joinHintUnfulfillable;
import static org.neo4j.notifications.NotificationCodeWithDescription.largeLabelLoadCsv;
import static org.neo4j.notifications.NotificationCodeWithDescription.missingLabel;
import static org.neo4j.notifications.NotificationCodeWithDescription.missingParameterForExplain;
import static org.neo4j.notifications.NotificationCodeWithDescription.missingPropertyName;
import static org.neo4j.notifications.NotificationCodeWithDescription.missingRelType;
import static org.neo4j.notifications.NotificationCodeWithDescription.noDatabasesReallocated;
import static org.neo4j.notifications.NotificationCodeWithDescription.procedureWarning;
import static org.neo4j.notifications.NotificationCodeWithDescription.repeatedRelationshipReference;
import static org.neo4j.notifications.NotificationCodeWithDescription.repeatedVarLengthRelationshipReference;
import static org.neo4j.notifications.NotificationCodeWithDescription.runtimeUnsupported;
import static org.neo4j.notifications.NotificationCodeWithDescription.serverAlreadyCordoned;
import static org.neo4j.notifications.NotificationCodeWithDescription.serverAlreadyEnabled;
import static org.neo4j.notifications.NotificationCodeWithDescription.sideEffectVisibility;
import static org.neo4j.notifications.NotificationCodeWithDescription.subqueryVariableShadowing;
import static org.neo4j.notifications.NotificationCodeWithDescription.unboundedShortestPath;
import static org.neo4j.notifications.NotificationCodeWithDescription.unionReturnOrder;
import static org.neo4j.notifications.NotificationCodeWithDescription.unsatisfiableRelationshipTypeExpression;
import static org.neo4j.notifications.NotificationDetail.repeatedRelationship;
import static org.neo4j.notifications.NotificationDetail.unsatisfiableRelTypeExpression;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;

class NotificationCodeWithDescriptionTest {
    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_node_index() {
        NotificationImplementation notification = indexHintUnfulfillable(
                InputPosition.empty,
                NotificationDetail.indexHint(EntityType.NODE, IndexHintIndexType.ANY, "person", "Person", "name"),
                NotificationDetail.index(IndexHintIndexType.ANY, "Person", List.of("name")));

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: INDEX FOR (`person`:`Person`) ON (`person`.`name`))",
                NotificationCategory.HINT,
                "Unable to create a plan with `INDEX :Person(name)` because the index does not exist.");
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_node_text_index() {
        NotificationImplementation notification = indexHintUnfulfillable(
                InputPosition.empty,
                NotificationDetail.indexHint(EntityType.NODE, IndexHintIndexType.TEXT, "person", "Person", "name"),
                NotificationDetail.index(IndexHintIndexType.TEXT, "Person", List.of("name")));

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: TEXT INDEX FOR (`person`:`Person`) ON (`person`.`name`))",
                NotificationCategory.HINT,
                "Unable to create a plan with `TEXT INDEX :Person(name)` because the index does not exist.");
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_rel_index() {
        NotificationImplementation notification = indexHintUnfulfillable(
                InputPosition.empty,
                NotificationDetail.indexHint(
                        EntityType.RELATIONSHIP, IndexHintIndexType.ANY, "person", "Person", "name"),
                NotificationDetail.index(IndexHintIndexType.ANY, "Person", List.of("name")));

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`))",
                NotificationCategory.HINT,
                "Unable to create a plan with `INDEX :Person(name)` because the index does not exist.");
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_rel_text_index() {
        NotificationImplementation notification = indexHintUnfulfillable(
                InputPosition.empty,
                NotificationDetail.indexHint(
                        EntityType.RELATIONSHIP, IndexHintIndexType.TEXT, "person", "Person", "name"),
                NotificationDetail.index(IndexHintIndexType.TEXT, "Person", List.of("name")));

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: TEXT INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`))",
                NotificationCategory.HINT,
                "Unable to create a plan with `TEXT INDEX :Person(name)` because the index does not exist.");
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_multiple_properties() {
        NotificationImplementation notification = indexHintUnfulfillable(
                InputPosition.empty,
                NotificationDetail.indexHint(
                        EntityType.RELATIONSHIP, IndexHintIndexType.TEXT, "person", "Person", "name", "age"),
                NotificationDetail.index(IndexHintIndexType.TEXT, "Person", List.of("name", "age")));

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: TEXT INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`, `person`.`age`))",
                NotificationCategory.HINT,
                "Unable to create a plan with `TEXT INDEX :Person(name, age)` because the index does not exist.");
    }

    @Test
    void shouldConstructNotificationFor_CARTESIAN_PRODUCT() {
        Set<String> idents = new TreeSet<>();
        idents.add("n");
        idents.add("node2");
        String identifierDetail = NotificationDetail.cartesianProductDescription(idents);
        NotificationImplementation notification =
                cartesianProduct(InputPosition.empty, identifierDetail, "(node1), (node)--(node2)");

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
                "The disconnected patterns `(node1), (node)--(node2)` build a cartesian product. "
                        + "A cartesian product may produce a large amount of data and slow down query processing.");
    }

    @Test
    void shouldConstructNotificationsFor_JOIN_HINT_UNFULFILLABLE() {
        NotificationImplementation notification = joinHintUnfulfillable(
                InputPosition.empty,
                NotificationDetail.joinKey(List.of("n", "node2")),
                NotificationDetail.commaSeparated(List.of("n", "node2")));

        verifyNotification(
                notification,
                "The database was unable to plan a hinted join.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.JoinHintUnfulfillableWarning",
                "The hinted join was not planned. This could happen because no generated plan contained the join key, "
                        + "please try using a different join key or restructure your query. "
                        + "(hinted join key identifiers are: n, node2)",
                NotificationCategory.HINT,
                "Unable to create a plan with `JOIN ON n, node2`. Try to change the join key(s) or restructure "
                        + "your query.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE() {
        String identifierDetail = NotificationDetail.deprecatedName("oldName", "newName");
        NotificationImplementation notification =
                deprecatedProcedureWithReplacement(InputPosition.empty, identifierDetail, "oldName", "newName");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated procedure. ('oldName' has been replaced by 'newName')",
                NotificationCategory.DEPRECATION,
                "`oldName` is deprecated. It is replaced by `newName`.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE_with_no_newName() {
        String identifierDetail = NotificationDetail.deprecatedName("oldName");
        NotificationImplementation notification =
                deprecatedProcedureWithoutReplacement(InputPosition.empty, identifierDetail, "oldName");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated procedure: `oldName`.",
                NotificationCategory.DEPRECATION,
                "`oldName` is deprecated and will be removed without a replacement.");
    }

    @Test
    void shouldConstructNotificationsFor_RUNTIME_UNSUPPORTED() {
        NotificationImplementation notification =
                runtimeUnsupported(InputPosition.empty, "runtime=parallel", "runtime=pipelined", "PARALLEL");

        verifyNotification(
                notification,
                "This query is not supported by the chosen runtime.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RuntimeUnsupportedWarning",
                "Selected runtime is unsupported for this query, please use a different runtime instead or fallback to default. (PARALLEL)",
                NotificationCategory.UNSUPPORTED,
                "The query cannot be executed with `runtime=parallel`, `runtime=pipelined` is used. Cause: `PARALLEL`.");
    }

    @Test
    void shouldConstructNotificationsFor_INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY() {
        NotificationImplementation notification = indexLookupForDynamicProperty(
                InputPosition.empty,
                NotificationDetail.nodeIndexSeekOrScan(Set.of("A")),
                NotificationDetail.commaSeparated(Set.of("A")));

        verifyNotification(
                notification,
                "Queries using dynamic properties will use neither index seeks nor index scans for those properties",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.DynamicProperty",
                "Using a dynamic property makes it impossible to use an index lookup for this query (indexed label is: (:A))",
                NotificationCategory.PERFORMANCE,
                "An index exists on label/type(s) `A`. It is not possible to use indexes for dynamic "
                        + "properties. Consider using static properties.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_FUNCTION() {
        String identifierDetail = NotificationDetail.deprecatedName("oldName", "newName");
        NotificationImplementation notification =
                deprecatedFunctionWithReplacement(InputPosition.empty, identifierDetail, "oldName", "newName");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated function. ('oldName' has been replaced by 'newName')",
                NotificationCategory.DEPRECATION,
                "`oldName` is deprecated. It is replaced by `newName`.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_FUNCTION_with_no_newName() {
        String identifierDetail = NotificationDetail.deprecatedName("oldName");
        NotificationImplementation notification =
                deprecatedFunctionWithoutReplacement(InputPosition.empty, identifierDetail, "oldName");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated function: `oldName`.",
                NotificationCategory.DEPRECATION,
                "`oldName` is deprecated and will be removed without a replacement.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_RUNTIME_OPTION() {
        NotificationImplementation notification = deprecatedRuntimeOption(
                InputPosition.empty, "option=deprecatedOption", "option=oldOption", "option=newOption");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated runtime option. (option=deprecatedOption)",
                NotificationCategory.DEPRECATION,
                "`option=oldOption` is deprecated. It is replaced by `option=newOption`.");
    }

    @Test
    void shouldConstructNotificationsFor_PROCEDURE_WARNING() {
        NotificationImplementation notification = procedureWarning(
                InputPosition.empty, "warning from procedure my.proc", "warning from procedure", "my.proc");

        verifyNotification(
                notification,
                "The query used a procedure that generated a warning.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Procedure.ProcedureWarning",
                "The query used a procedure that generated a warning. (warning from procedure my.proc)",
                NotificationCategory.GENERIC,
                "The procedure `my.proc` generates the warning `warning from procedure`.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE_RETURN_FIELD() {
        NotificationImplementation notification = deprecatedProcedureReturnField(
                InputPosition.empty, "'field' returned by 'proc' is deprecated.", "proc", "field");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated field from a procedure. ('field' returned by 'proc' is deprecated.)",
                NotificationCategory.DEPRECATION,
                "`field` returned by procedure `proc` is deprecated.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR() {
        NotificationImplementation notification =
                deprecatedRelationshipTypeSeparator(InputPosition.empty, "Please use ':A|B' instead", ":A:|B", ":A|B");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The semantics of using colon in the separation of alternative relationship types will change in a future version. (Please use ':A|B' instead)",
                NotificationCategory.DEPRECATION,
                "`:A:|B` is deprecated. It is replaced by `:A|B`.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE() {
        NotificationImplementation notification =
                deprecatedNodeOrRelationshipOnRhsSetClause(InputPosition.empty, "SET a = b", "SET a = properties(b)");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The use of nodes or relationships for setting properties is deprecated and will be removed in a future version. "
                        + "Please use properties() instead.",
                NotificationCategory.DEPRECATION,
                "`SET a = b` is deprecated. It is replaced by `SET a = properties(b)`.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP() {
        NotificationImplementation notification = deprecatedShortestPathWithFixedLengthRelationship(
                InputPosition.empty, "shortestPath((n)-[r]->(m))", "shortestPath((n)-[r*1..1]->(m))");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The use of shortestPath and allShortestPaths with fixed length relationships is deprecated and will be removed in a future version. "
                        + "Please use a path with a length of 1 [r*1..1] instead or a Match with a limit.",
                NotificationCategory.DEPRECATION,
                "`shortestPath((n)-[r]->(m))` is deprecated. It is replaced by `shortestPath((n)-[r*1..1]->(m))`.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_TEXT_INDEX_PROVIDER() {
        NotificationImplementation notification = deprecatedTextIndexProvider(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The `text-1.0` provider for text indexes is deprecated and will be removed in a future version. Please use `text-2.0` instead.",
                NotificationCategory.DEPRECATION,
                "`text-1.0` is deprecated. It is replaced by `text-2.0`.");
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
                "The query execution plan contains the `Eager` operator. `LOAD CSV` in combination with "
                        + "`Eager` can consume a lot of memory.");
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
                "`deprecatedFormat` is deprecated. It is replaced by `newFormat`.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_IDENTIFIER_WHITESPACE_UNICODE() {
        NotificationImplementation notification =
                deprecatedIdentifierWhitespaceUnicode(InputPosition.empty, 'a', "ana");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The Unicode character `\\u0061` is deprecated for unescaped identifiers and will be considered as a whitespace character in the future. To continue using it, escape the identifier by adding backticks around the identifier `ana`.",
                NotificationCategory.DEPRECATION,
                "The Unicode character `\\u0061` is deprecated for unescaped identifiers and will be considered as a whitespace character in the future. To continue using it, escape the identifier by adding backticks around the identifier `ana`.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_IDENTIFIER_UNICODE() {
        NotificationImplementation notification = deprecatedIdentifierUnicode(InputPosition.empty, 'a', "ana");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The character with the Unicode representation `\\u0061` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `ana`.",
                NotificationCategory.DEPRECATION,
                "The character with the Unicode representation `\\u0061` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `ana`.");
    }

    @Test
    void shouldConstructNotificationsFor_LARGE_LABEL_LOAD_CSV() {
        NotificationImplementation notification = largeLabelLoadCsv(InputPosition.empty, "Label");

        verifyNotification(
                notification,
                "Adding a schema index may speed up this query.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.NoApplicableIndex",
                "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely "
                        + "not perform well on large data sets. Please consider using a schema index.",
                NotificationCategory.PERFORMANCE,
                "`LOAD CSV` in combination with `MATCH` or `MERGE` on a label that does not have an index may "
                        + "result in long execution times. Consider adding an index for label `Label`.");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_LABEL() {
        NotificationImplementation notification =
                missingLabel(InputPosition.empty, NotificationDetail.missingLabel("Label"), "Label");

        verifyNotification(
                notification,
                "The provided label is not in the database.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownLabelWarning",
                "One of the labels in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application (the missing label name is: Label)",
                NotificationCategory.UNRECOGNIZED,
                "The label `Label` does not exist. Verify that the spelling is correct.");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_REL_TYPE() {
        NotificationImplementation notification =
                missingRelType(InputPosition.empty, NotificationDetail.missingRelationshipType("Rel"), "Rel");

        verifyNotification(
                notification,
                "The provided relationship type is not in the database.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownRelationshipTypeWarning",
                "One of the relationship types in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application (the missing relationship type is: Rel)",
                NotificationCategory.UNRECOGNIZED,
                "The relationship type `Rel` does not exist. Verify that the spelling is correct.");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PROPERTY_NAME() {
        NotificationImplementation notification =
                missingPropertyName(InputPosition.empty, NotificationDetail.propertyName("prop"), "prop");

        verifyNotification(
                notification,
                "The provided property key is not in the database",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownPropertyKeyWarning",
                "One of the property names in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application "
                        + "(the missing property name is: prop)",
                NotificationCategory.UNRECOGNIZED,
                "The property `prop` does not exist. Verify that the spelling is correct.");
    }

    @Test
    void shouldConstructNotificationsFor_UNBOUNDED_SHORTEST_PATH() {

        String pattern = "(a)-[2..*]-(b)";
        NotificationImplementation notification = unboundedShortestPath(InputPosition.empty, pattern);

        verifyNotification(
                notification,
                "The provided pattern is unbounded, consider adding an upper limit to the number of node hops.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.UnboundedVariableLengthPattern",
                "Using shortest path with an unbounded pattern will likely result in long execution times. "
                        + "It is recommended to use an upper limit to the number of node hops in your pattern.",
                NotificationCategory.PERFORMANCE,
                "The provided pattern `(a)-[2..*]-(b)` is unbounded. Shortest path with an unbounded pattern may result"
                        + " in long execution times. Use an upper limit (e.g. `[*..5]`) on the number of node hops in your pattern.");
    }

    @Test
    void shouldConstructNotificationsFor_EXHAUSTIVE_SHORTEST_PATH() {
        NotificationImplementation notification =
                exhaustiveShortestPath(InputPosition.empty, NotificationDetail.commaSeparated(Set.of("length(p) > 1")));

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
                "The query runs with exhaustive shortest path due to the existential predicate(s) `length(p) > 1`. "
                        + "It may be possible to use `WITH` to separate the `MATCH` from the existential predicate(s).");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PARAMETER_FOR_EXPLAIN() {
        NotificationImplementation notification = missingParameterForExplain(
                InputPosition.empty,
                NotificationDetail.missingParameters(List.of("param1")),
                NotificationDetail.parameters(List.of("param1")));

        verifyNotification(
                notification,
                "The statement refers to a parameter that was not provided in the request.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.ParameterNotProvided",
                "Did not supply query with enough parameters. "
                        + "The produced query plan will not be cached and is not executable without EXPLAIN. (Missing parameters: param1)",
                NotificationCategory.GENERIC,
                "The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined "
                        + "parameter(s) `$param1`. Provide the parameter(s).");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PARAMETERS_FOR_EXPLAIN() {
        NotificationImplementation notification = missingParameterForExplain(
                InputPosition.empty,
                NotificationDetail.missingParameters(List.of("param1", "param2")),
                NotificationDetail.parameters(List.of("param1", "param2")));

        verifyNotification(
                notification,
                "The statement refers to a parameter that was not provided in the request.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.ParameterNotProvided",
                "Did not supply query with enough parameters. "
                        + "The produced query plan will not be cached and is not executable without EXPLAIN. (Missing parameters: param1, param2)",
                NotificationCategory.GENERIC,
                "The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined "
                        + "parameter(s) `$param1, $param2`. Provide the parameter(s).");
    }

    @Test
    void shouldConstructNotificationsFor_CODE_GENERATION_FAILED() {
        NotificationImplementation notification = codeGenerationFailed(
                InputPosition.empty,
                "runtime=pipelined operatorEngine=compiled expressionEngine=compiled",
                "runtime=pipelined operatorEngine=interpreted expressionEngine=compiled",
                "method too big");

        verifyNotification(
                notification,
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.CodeGenerationFailed",
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log. (method too big)",
                NotificationCategory.PERFORMANCE,
                "The query cannot be executed with `runtime=pipelined operatorEngine=compiled expressionEngine=compiled`, "
                        + "`runtime=pipelined operatorEngine=interpreted expressionEngine=compiled` is used. Cause: `method too big`.");
    }

    @Test
    void shouldConstructNotificationsFor_SUBQUERY_VARIABLE_SHADOWING() {
        NotificationImplementation notification =
                subqueryVariableShadowing(InputPosition.empty, NotificationDetail.shadowingVariable("v"), "v");

        verifyNotification(
                notification,
                "Variable in subquery is shadowing a variable with the same name from the outer scope.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.SubqueryVariableShadowing",
                "Variable in subquery is shadowing a variable with the same name from the outer scope. "
                        + "If you want to use that variable instead, it must be imported into the subquery using "
                        + "importing WITH clause. (the shadowing variable is: v)",
                NotificationCategory.GENERIC,
                "The variable `v` in the subquery uses the same name as a variable from the outer query. Use "
                        + "`WITH v` in the subquery to import the one from the outer scope unless you want it to be a new variable.");
    }

    @Test
    void shouldConstructNotificationsFor_HOME_DATABASE_NOT_PRESENT() {
        NotificationImplementation notification = homeDatabaseNotPresent(InputPosition.empty, "db", "db");

        verifyNotification(
                notification,
                "The request referred to a home database that does not exist.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Database.HomeDatabaseNotFound",
                "The home database provided does not currently exist in the DBMS. "
                        + "This command will not take effect until this database is created. (db)",
                NotificationCategory.UNRECOGNIZED,
                "The database `db` does not exist. Verify that the spelling is correct or create the database for the command to take effect.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_DATABASE_NAME() {
        NotificationImplementation notification = deprecatedDatabaseName(InputPosition.empty, "Name: db.one");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "Databases and aliases with unescaped `.` are deprecated unless to indicate that they belong to a composite database. "
                        + "Names containing `.` should be escaped. (Name: db.one)",
                NotificationCategory.DEPRECATION,
                "Databases and aliases with unescaped `.` are deprecated unless they belong to a composite database. "
                        + "Names containing `.` should be escaped. (Name: db.one)");
    }

    @Test
    void shouldConstructNotificationsFor_UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION() {
        NotificationImplementation notification = unsatisfiableRelationshipTypeExpression(
                InputPosition.empty, unsatisfiableRelTypeExpression("!%"), "!%");

        verifyNotification(
                notification,
                "The query contains a relationship type expression that cannot be satisfied.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnsatisfiableRelationshipTypeExpression",
                "Relationship type expression cannot possibly be satisfied. (`!%` can never be satisfied by any relationship. Relationships must have exactly one relationship type.)",
                NotificationCategory.GENERIC,
                "The expression `!%` cannot be satisfied because relationships must have exactly one type.");
    }

    @Test
    void shouldConstructNotificationsFor_REPEATED_RELATIONSHIP_REFERENCE() {
        NotificationImplementation notification = repeatedRelationshipReference(
                InputPosition.empty, repeatedRelationship("r"), "r", "()-[r]->()<-[r]-()");

        verifyNotification(
                notification,
                "The query returns no results due to repeated references to a relationship.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RepeatedRelationshipReference",
                "A relationship is referenced more than once in the query, which leads to no results because relationships must not occur more than once in each result. (Relationship `r` was repeated)",
                NotificationCategory.GENERIC,
                "`r` is repeated in `()-[r]->()<-[r]-()`, which leads to no results.");
    }

    @Test
    void shouldConstructNotificationsFor_REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE() {
        NotificationImplementation notification = repeatedVarLengthRelationshipReference(
                InputPosition.empty, repeatedRelationship("r"), "r", "()-[r*]->()-[r*]->()");

        verifyNotification(
                notification,
                "The query returns no results due to repeated references to a relationship.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RepeatedRelationshipReference",
                "A variable-length relationship variable is bound more than once, which leads to no results because relationships must not occur more than once in each result. (Relationship `r` was repeated)",
                NotificationCategory.GENERIC,
                "`r` is repeated in `()-[r*]->()-[r*]->()`, which leads to no results.");
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
                "All subqueries in a UNION [ALL] should have the same ordering for the return columns. Using differently ordered return items in a UNION [ALL] clause is deprecated and will be removed in a future version.");
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
                "`connectComponentsPlanner` is deprecated and will be removed without a replacement.");
    }

    @Test
    void shouldConstructNotificationsFor_COMMAND_HAD_NO_EFFECT_ASSIGN_PRIVILEGE() {
        NotificationImplementation notification =
                commandHasNoEffectAssignPrivilege(InputPosition.empty, "GRANT WRITE ON GRAPH * TO editor");

        verifyNotification(
                notification,
                "`GRANT WRITE ON GRAPH * TO editor` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Security.CommandHasNoEffect",
                "The role already has the privilege. See Status Codes documentation for more information.",
                NotificationCategory.SECURITY,
                "`GRANT WRITE ON GRAPH * TO editor` has no effect. The role already has the privilege.");
    }

    @Test
    void shouldConstructNotificationsFor_COMMAND_HAD_NO_EFFECT_REVOKE_PRIVILEGE() {
        NotificationImplementation notification = commandHasNoEffectRevokePrivilege(
                InputPosition.empty, "REVOKE ALL GRAPH PRIVILEGES ON GRAPH * FROM reader");

        verifyNotification(
                notification,
                "`REVOKE ALL GRAPH PRIVILEGES ON GRAPH * FROM reader` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Security.CommandHasNoEffect",
                "The role does not have the privilege. See Status Codes documentation for more information.",
                NotificationCategory.SECURITY,
                "`REVOKE ALL GRAPH PRIVILEGES ON GRAPH * FROM reader` has no effect. The role does not have the privilege.");
    }

    @Test
    void shouldConstructNotificationsFor_COMMAND_HAD_NO_EFFECT_GRANT_ROLE() {
        NotificationImplementation notification =
                commandHasNoEffectGrantRole(InputPosition.empty, "GRANT ROLE aliceRole TO alice");

        verifyNotification(
                notification,
                "`GRANT ROLE aliceRole TO alice` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Security.CommandHasNoEffect",
                "The user already has the role. See Status Codes documentation for more information.",
                NotificationCategory.SECURITY,
                "`GRANT ROLE aliceRole TO alice` has no effect. The user already has the role.");
    }

    @Test
    void shouldConstructNotificationsFor_COMMAND_HAD_NO_EFFECT_REVOKE_ROLE() {
        NotificationImplementation notification =
                commandHasNoEffectRevokeRole(InputPosition.empty, "REVOKE ROLE other FROM alice");

        verifyNotification(
                notification,
                "`REVOKE ROLE other FROM alice` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Security.CommandHasNoEffect",
                "The user does not have the role. See Status Codes documentation for more information.",
                NotificationCategory.SECURITY,
                "`REVOKE ROLE other FROM alice` has no effect. The user does not have the role.");
    }

    @Test
    void shouldConstructNotificationsFor_IMPOSSIBLE_REVOKE_COMMAND() {
        NotificationImplementation notification =
                impossibleRevokeCommand(InputPosition.empty, "REVOKE admina FROM ALICE", "Role does not exist.");

        verifyNotification(
                notification,
                "`REVOKE admina FROM ALICE` has no effect.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Security.ImpossibleRevokeCommand",
                "Role does not exist. Make sure nothing is misspelled. This notification will become an error in a future major version. "
                        + "See Status Codes documentation for more information.",
                NotificationCategory.SECURITY,
                "`REVOKE admina FROM ALICE` has no effect. Role does not exist. Make sure nothing is misspelled. This notification will become an error in a future major version.");
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
                "Creating an entity (n.prop) and referencing that entity in a property definition in the same CREATE is deprecated.");
    }

    @Test
    void shouldConstructNotificationsFor_SERVER_ALREADY_ENABLED() {
        NotificationImplementation notification = serverAlreadyEnabled(InputPosition.empty, "server");

        verifyNotification(
                notification,
                "`ENABLE SERVER` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Cluster.ServerAlreadyEnabled",
                "Server `server` is already enabled. Verify that this is the intended server.",
                NotificationCategory.TOPOLOGY,
                "`ENABLE SERVER` has no effect. Server `server` is already enabled. Verify that this is the intended server.");
    }

    @Test
    void shouldConstructNotificationsFor_SERVER_ALREADY_CORDONED() {
        NotificationImplementation notification = serverAlreadyCordoned(InputPosition.empty, "server");

        verifyNotification(
                notification,
                "`CORDON SERVER` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Cluster.ServerAlreadyCordoned",
                "Server `server` is already cordoned. Verify that this is the intended server.",
                NotificationCategory.TOPOLOGY,
                "`CORDON SERVER` has no effect. Server `server` is already cordoned. Verify that this is the intended server.");
    }

    @Test
    void shouldConstructNotificationsFor_NO_DATABASES_REALLOCATED() {
        NotificationImplementation notification = noDatabasesReallocated(InputPosition.empty);

        verifyNotification(
                notification,
                "`REALLOCATE DATABASES` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Cluster.NoDatabasesReallocated",
                "No databases were reallocated. No better allocation is currently possible.",
                NotificationCategory.TOPOLOGY,
                "`REALLOCATE DATABASES` has no effect. No databases were reallocated. No better allocation is currently possible.");
    }

    @Test
    void shouldConstructNotificationsFor_SIDE_EFFECT_VISIBILITY() {
        NotificationImplementation notification = sideEffectVisibility(InputPosition.empty);

        verifyNotification(
                notification,
                "Using a subquery expression within a mutating statement has implications for its side-effect visibility",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.SideEffectVisibility",
                "The semantics of this statement may change in later versions."
                        + " To overcome this, extract the subquery expression into a preceding WITH and potentially wrap the mutating statement into a CALL subquery.",
                NotificationCategory.DEPRECATION,
                "Using a subquery expression within a mutating statement has implications for its side-effect visibility."
                        + " The semantics of this statement may change in later versions."
                        + " To overcome this, extract the subquery expression into a preceding WITH and potentially wrap the mutating statement into a CALL subquery.");
    }

    @Test
    void shouldConstructNotificationsFor_CORDONED_SERVERS_EXIST() {
        NotificationImplementation notification =
                cordonedServersExist(InputPosition.empty, "server-1,server-2,server-3");

        verifyNotification(
                notification,
                "Cordoned servers existed when making an allocation decision.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Cluster.CordonedServersExistedDuringAllocation",
                "Server(s) `server-1,server-2,server-3` are cordoned. This can impact allocation decisions.",
                NotificationCategory.TOPOLOGY,
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
            -28, -71, 38, 54, 97, -112, -5, 12, 103, -124, 85, -73, 94, 31, -10, -92, 23, 87, -3, -79, -8, -16, -128,
            90, -88, 126, -63, -34, -48, 95, 20, -83
        };

        if (!Arrays.equals(notificationHash, expectedHash)) {
            fail("Expected: " + Arrays.toString(expectedHash) + " \n Actual: " + Arrays.toString(notificationHash)
                    + "\n If you have added, changed or removed a notification, "
                    + "please follow the process on https://trello.com/c/9L3lbeSY/27-update-to-notification-name");
        }
    }
}
