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

import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.notifications.NotificationCodeWithDescription.aggregationSkippedNull;
import static org.neo4j.notifications.NotificationCodeWithDescription.authProviderNotDefined;
import static org.neo4j.notifications.NotificationCodeWithDescription.callableShadowing;
import static org.neo4j.notifications.NotificationCodeWithDescription.cartesianProduct;
import static org.neo4j.notifications.NotificationCodeWithDescription.codeGenerationFailed;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectAssignPrivilege;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectGrantRole;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectGrantRoleToAuthRule;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectRevokePrivilege;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectRevokeRole;
import static org.neo4j.notifications.NotificationCodeWithDescription.commandHasNoEffectRevokeRoleToAuthRule;
import static org.neo4j.notifications.NotificationCodeWithDescription.cordonedServersExist;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedBooleanCoercion;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedConnectComponentsPlannerPreParserOption;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedEagerAnalyzerPreParserOption;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedExistingDataOption;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFormat;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionNamespace;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionWithReplacement;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedFunctionWithoutReplacement;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedGraphReferenceNotification;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedIdentifierUnicode;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedIdentifierWhitespaceUnicode;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedImportingWithInSubqueryCall;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedIndexProviderOption;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedKeywordVariableInWhenOperand;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedNodeOrRelationshipOnRhsSetClause;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedOptionInOptionMap;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedPrecedenceOfLabelExpressionPredicate;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureNamespace;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureWithReplacement;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedProcedureWithoutReplacement;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedPropertyReferenceInCreate;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedPropertyReferenceInMerge;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedQuotedGraphByNameArgument;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedRelationshipTypeSeparator;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedRequestedFeature;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedRuntimeOption;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedSeedingOption;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedShortestPathWithFixedLengthRelationship;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedStoreFormat;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedTextIndexProvider;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedWhereVariableInNodePattern;
import static org.neo4j.notifications.NotificationCodeWithDescription.deprecatedWhereVariableInRelationshipPattern;
import static org.neo4j.notifications.NotificationCodeWithDescription.eagerLoadCsv;
import static org.neo4j.notifications.NotificationCodeWithDescription.exhaustiveShortestPath;
import static org.neo4j.notifications.NotificationCodeWithDescription.externalAuthNotEnabled;
import static org.neo4j.notifications.NotificationCodeWithDescription.homeDatabaseNotPresent;
import static org.neo4j.notifications.NotificationCodeWithDescription.identifierShadowingVariable;
import static org.neo4j.notifications.NotificationCodeWithDescription.impossibleRevokeCommand;
import static org.neo4j.notifications.NotificationCodeWithDescription.indexHintUnfulfillable;
import static org.neo4j.notifications.NotificationCodeWithDescription.indexLookupForDynamicProperty;
import static org.neo4j.notifications.NotificationCodeWithDescription.indexOrConstraintAlreadyExists;
import static org.neo4j.notifications.NotificationCodeWithDescription.indexOrConstraintDoesNotExist;
import static org.neo4j.notifications.NotificationCodeWithDescription.insecureProtocol;
import static org.neo4j.notifications.NotificationCodeWithDescription.joinHintUnfulfillable;
import static org.neo4j.notifications.NotificationCodeWithDescription.largeLabelLoadCsv;
import static org.neo4j.notifications.NotificationCodeWithDescription.missingLabel;
import static org.neo4j.notifications.NotificationCodeWithDescription.missingParameterForExplain;
import static org.neo4j.notifications.NotificationCodeWithDescription.missingPropertyName;
import static org.neo4j.notifications.NotificationCodeWithDescription.missingRelType;
import static org.neo4j.notifications.NotificationCodeWithDescription.noDatabasesReallocated;
import static org.neo4j.notifications.NotificationCodeWithDescription.procedureWarning;
import static org.neo4j.notifications.NotificationCodeWithDescription.redundantOptionalProcedure;
import static org.neo4j.notifications.NotificationCodeWithDescription.redundantOptionalSubquery;
import static org.neo4j.notifications.NotificationCodeWithDescription.repeatedRelationshipReference;
import static org.neo4j.notifications.NotificationCodeWithDescription.repeatedVarLengthRelationshipReference;
import static org.neo4j.notifications.NotificationCodeWithDescription.requestedTopologyMatchedCurrentTopology;
import static org.neo4j.notifications.NotificationCodeWithDescription.retiredPlannerVersionPreParserOption;
import static org.neo4j.notifications.NotificationCodeWithDescription.runtimeUnsupported;
import static org.neo4j.notifications.NotificationCodeWithDescription.serverAlreadyCordoned;
import static org.neo4j.notifications.NotificationCodeWithDescription.serverAlreadyEnabled;
import static org.neo4j.notifications.NotificationCodeWithDescription.shadowingInternalFunction;
import static org.neo4j.notifications.NotificationCodeWithDescription.subqueryVariableShadowing;
import static org.neo4j.notifications.NotificationCodeWithDescription.unboundedShortestPath;
import static org.neo4j.notifications.NotificationCodeWithDescription.unsatisfiableRelationshipTypeExpression;
import static org.neo4j.notifications.NotificationCodeWithDescription.vectorIndexDimensionsNotSpecified;
import static org.neo4j.notifications.NotificationCodeWithDescription.waitServerCatchingUp;
import static org.neo4j.notifications.NotificationCodeWithDescription.waitServerCaughtUp;
import static org.neo4j.notifications.NotificationCodeWithDescription.waitServerFailed;
import static org.neo4j.notifications.NotificationCodeWithDescription.waitServerUnavailable;
import static org.neo4j.notifications.NotificationDetail.repeatedRelationship;
import static org.neo4j.notifications.NotificationDetail.unsatisfiableRelTypeExpression;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType;
import org.neo4j.gqlstatus.DiagnosticRecord;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.gqlstatus.NotificationClassification;
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
                NotificationClassification.HINT,
                "01N31",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.HINT,
                                -1,
                                -1,
                                -1,
                                Map.of("idxDescr", "INDEX :Person(name)"))
                        .asMap(),
                "warn: hinted index does not exist. Unable to create a plan with 'INDEX :Person(name)' because the index does not exist.");
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
                NotificationClassification.HINT,
                "01N31",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.HINT,
                                -1,
                                -1,
                                -1,
                                Map.of("idxDescr", "TEXT INDEX :Person(name)"))
                        .asMap(),
                "warn: hinted index does not exist. Unable to create a plan with 'TEXT INDEX :Person(name)' because the index does not exist.");
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
                NotificationClassification.HINT,
                "01N31",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.HINT,
                                -1,
                                -1,
                                -1,
                                Map.of("idxDescr", "INDEX :Person(name)"))
                        .asMap(),
                "warn: hinted index does not exist. Unable to create a plan with 'INDEX :Person(name)' because the index does not exist.");
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
                NotificationClassification.HINT,
                "01N31",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.HINT,
                                -1,
                                -1,
                                -1,
                                Map.of("idxDescr", "TEXT INDEX :Person(name)"))
                        .asMap(),
                "warn: hinted index does not exist. Unable to create a plan with 'TEXT INDEX :Person(name)' because the index does not exist.");
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
                NotificationClassification.HINT,
                "01N31",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.HINT,
                                -1,
                                -1,
                                -1,
                                Map.of("idxDescr", "TEXT INDEX :Person(name, age)"))
                        .asMap(),
                "warn: hinted index does not exist. Unable to create a plan with 'TEXT INDEX :Person(name, age)' because the index does not exist.");
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
                NotificationClassification.PERFORMANCE,
                "03N90",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.PERFORMANCE,
                                -1,
                                -1,
                                -1,
                                Map.of("pat", "(node1), (node)--(node2)"))
                        .asMap(),
                "info: cartesian product. The disconnected pattern '(node1), (node)--(node2)' builds a cartesian product. A cartesian product may produce a large amount of data and slow down query processing.");
    }

    @Test
    void shouldConstructNotificationsFor_JOIN_HINT_UNFULFILLABLE() {
        NotificationImplementation notification = joinHintUnfulfillable(
                InputPosition.empty, NotificationDetail.joinKey(List.of("n", "node2")), List.of("n", "node2"));

        verifyNotification(
                notification,
                "The database was unable to plan a hinted join.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.JoinHintUnfulfillableWarning",
                "The hinted join was not planned. This could happen because no generated plan contained the join key, "
                        + "please try using a different join key or restructure your query. "
                        + "(hinted join key identifiers are: n, node2)",
                NotificationCategory.HINT,
                NotificationClassification.HINT,
                "01N30",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.HINT,
                                -1,
                                -1,
                                -1,
                                Map.of("variableList", List.of("n", "node2")))
                        .asMap(),
                "warn: join hint unfulfillable. Unable to create a plan with 'JOIN ON `n`, `node2`'. Try to change the join key(s) or restructure your query.");
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
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "oldName", "feat2", "newName"))
                        .asMap(),
                "warn: feature deprecated with replacement. oldName is deprecated. It is replaced by newName.");
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
                NotificationClassification.DEPRECATION,
                "01N02",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("feat", "oldName"))
                        .asMap(),
                "warn: feature deprecated without replacement. oldName is deprecated and will be removed without a replacement.");
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
                NotificationClassification.UNSUPPORTED,
                "01N40",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.UNSUPPORTED,
                                -1,
                                -1,
                                -1,
                                Map.of(
                                        "preparserInput1",
                                        "runtime=parallel",
                                        "preparserInput2",
                                        "runtime=pipelined",
                                        "msg",
                                        "PARALLEL"))
                        .asMap(),
                "warn: unsupported runtime. The query cannot be executed with 'runtime=parallel'; instead, 'runtime=pipelined' is used. Cause: PARALLEL.");
    }

    @Test
    void shouldConstructNotificationsFor_INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY() {
        NotificationImplementation notification = indexLookupForDynamicProperty(
                InputPosition.empty, NotificationDetail.nodeIndexSeekOrScan(Set.of("A")), List.of("A"));

        verifyNotification(
                notification,
                "Queries using dynamic properties will use neither index seeks nor index scans for those properties",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.DynamicProperty",
                "Using a dynamic property makes it impossible to use an index lookup for this query (indexed label is: (:A))",
                NotificationCategory.PERFORMANCE,
                NotificationClassification.PERFORMANCE,
                "03N95",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.PERFORMANCE,
                                -1,
                                -1,
                                -1,
                                Map.of("labelList", List.of("A")))
                        .asMap(),
                "info: dynamic property. An index already exists on the relationship type or the label(s) `A`. It is not possible to use indexes for dynamic properties. Consider using static properties.");
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
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "oldName", "feat2", "newName"))
                        .asMap(),
                "warn: feature deprecated with replacement. oldName is deprecated. It is replaced by newName.");
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
                NotificationClassification.DEPRECATION,
                "01N02",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("feat", "oldName"))
                        .asMap(),
                "warn: feature deprecated without replacement. oldName is deprecated and will be removed without a replacement.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_FUNCTION_NAMESPACE() {
        NotificationImplementation notification = deprecatedFunctionNamespace(InputPosition.empty, "point.function");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The namespace of the invoked user-defined function is deprecated. (point.function)",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "oldName", "feat2", "newName"))
                        .asMap(),
                "warn: feature deprecated. The namespace used by the user-defined function `point.function` is deprecated.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE_NAMESPACE() {
        NotificationImplementation notification = deprecatedProcedureNamespace(InputPosition.empty, "point.procedure");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The namespace of the called user-defined procedure is deprecated. (point.procedure)",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "oldName", "feat2", "newName"))
                        .asMap(),
                "warn: feature deprecated. The namespace used by the user-defined procedure `point.procedure` is deprecated.");
    }

    @Test
    void shouldConstructNotificationsFor_SHADOWING_INTERNAL_FUNCTION() {
        NotificationImplementation notification = shadowingInternalFunction(InputPosition.empty, "point.function");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The namespace of the invoked user-defined function is deprecated and the function is shadowing an internal function. (point.function)",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "oldName", "feat2", "newName"))
                        .asMap(),
                "warn: feature deprecated. The namespace of the invoked user-defined function `point.function` is deprecated and the function is shadowing an internal function.");
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
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "option=oldOption", "feat2", "option=newOption"))
                        .asMap(),
                "warn: feature deprecated with replacement. option=oldOption is deprecated. It is replaced by option=newOption.");
    }

    @Test
    void shouldConstructNotificationsFor_PROCEDURE_WARNING() {
        NotificationImplementation notification = procedureWarning(
                InputPosition.empty, "warning from procedure my.proc", "Warning from procedure.", "my.proc");

        verifyNotification(
                notification,
                "The query used a procedure that generated a warning.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Procedure.ProcedureWarning",
                "The query used a procedure that generated a warning. (warning from procedure my.proc)",
                NotificationCategory.GENERIC,
                NotificationClassification.GENERIC,
                "01N62",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.GENERIC,
                                -1,
                                -1,
                                -1,
                                Map.of("proc", "my.proc", "msg", "Warning from procedure."))
                        .asMap(),
                "warn: procedure or function execution warning. Execution of the procedure my.proc() generated the warning Warning from procedure.");
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
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "':A:|B'", "feat2", "':A|B'"))
                        .asMap(),
                "warn: feature deprecated with replacement. ':A:|B' is deprecated. It is replaced by ':A|B'.");
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
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "'SET a = b'", "feat2", "'SET a = properties(b)'"))
                        .asMap(),
                "warn: feature deprecated with replacement. 'SET a = b' is deprecated. It is replaced by 'SET a = properties(b)'.");
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
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of(
                                        "feat1",
                                        "shortestPath((n)-[r]->(m))",
                                        "feat2",
                                        "shortestPath((n)-[r*1..1]->(m))"))
                        .asMap(),
                "warn: feature deprecated with replacement. shortestPath((n)-[r]->(m)) is deprecated. It is replaced by shortestPath((n)-[r*1..1]->(m)).");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_TEXT_INDEX_PROVIDER() {
        NotificationImplementation notification = deprecatedTextIndexProvider(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "`text-1.0`, `text-2.0` providers for text indexes are deprecated and will be removed in a future version. Please use `text-3.0` instead.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "text-1.0", "feat2", "text-2.0"))
                        .asMap(),
                "warn: feature deprecated with replacement. text-1.0 is deprecated. It is replaced by text-2.0.");
    }

    @Test
    void shouldConstructNotificationFor_DEPRECATED_INDEX_PROVIDER_OPTION() {
        NotificationImplementation notification = deprecatedIndexProviderOption(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The `indexProvider` option is deprecated and will be removed in a future version. Neo4j does not use the given option but instead selects the most performant index provider available.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of(
                                        "item",
                                        "The `indexProvider` option is deprecated and will be removed in a future version. Neo4j does not use the given option but instead selects the most performant index provider available."))
                        .asMap(),
                "warn: feature deprecated. The `indexProvider` option is deprecated and will be removed in a future version. Neo4j does not use the given option but instead selects the most performant index provider available.");
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
                NotificationClassification.PERFORMANCE,
                "03N94",
                new DiagnosticRecord(info, NotificationClassification.PERFORMANCE, -1, -1, -1, Map.of()).asMap(),
                "info: eager operator. The query execution plan contains the 'Eager' operator. 'LOAD CSV' in combination with 'Eager' can consume a lot of memory.");
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
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "deprecatedFormat", "feat2", "newFormat"))
                        .asMap(),
                "warn: feature deprecated with replacement. deprecatedFormat is deprecated. It is replaced by newFormat.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_REQUESTED_FEATURE() {
        NotificationImplementation notification =
                deprecatedRequestedFeature(InputPosition.empty, "HTTP API", "Query API");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Request.FeatureDeprecationWarning",
                "HTTP API is deprecated. It is replaced by Query API.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of()).asMap(),
                "warn: feature deprecated with replacement. HTTP API is deprecated. It is replaced by Query API.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_IDENTIFIER_WHITESPACE_UNICODE() {
        NotificationImplementation notification =
                deprecatedIdentifierWhitespaceUnicode(InputPosition.empty, 'a', "ana");

        String message =
                "The Unicode character `\\u0061` is deprecated for unescaped identifiers and will be considered as a whitespace character in the future. To continue using it, escape the identifier by adding backticks around the identifier `ana`.";
        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                message,
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("item", message))
                        .asMap(),
                String.format("warn: feature deprecated. %s", message));
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_IDENTIFIER_UNICODE() {
        NotificationImplementation notification = deprecatedIdentifierUnicode(InputPosition.empty, 'a', "ana");
        String message =
                "The character with the Unicode representation `\\u0061` is deprecated for unescaped identifiers and will not be supported in the future. To continue using it, escape the identifier by adding backticks around the identifier `ana`.";

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                message,
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("item", message))
                        .asMap(),
                String.format("warn: feature deprecated. %s", message));
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
                NotificationClassification.PERFORMANCE,
                "03N93",
                new DiagnosticRecord(info, NotificationClassification.PERFORMANCE, -1, -1, -1, Map.of("label", "Label"))
                        .asMap(),
                "info: no applicable index. 'LOAD CSV' in combination with 'MATCH' or 'MERGE' on a label that does not have an index may result in long execution times. Consider adding an index for label `Label`.");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_LABEL() {
        NotificationImplementation notification =
                missingLabel(InputPosition.empty, NotificationDetail.missingLabel("Label"), "Label", "myDb");

        verifyNotification(
                notification,
                "The provided label is not in the database.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownLabelWarning",
                "One of the labels in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application (the missing label name is: Label)",
                NotificationCategory.UNRECOGNIZED,
                NotificationClassification.UNRECOGNIZED,
                "01N50",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.UNRECOGNIZED,
                                -1,
                                -1,
                                -1,
                                Map.of("label", "Label", "db", "myDb"))
                        .asMap(),
                "warn: label does not exist. The label `Label` does not exist in database `myDb`. Verify that the spelling is correct.");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_REL_TYPE() {
        NotificationImplementation notification =
                missingRelType(InputPosition.empty, NotificationDetail.missingRelationshipType("Rel"), "Rel", "neo4j");

        verifyNotification(
                notification,
                "The provided relationship type is not in the database.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownRelationshipTypeWarning",
                "One of the relationship types in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application (the missing relationship type is: Rel)",
                NotificationCategory.UNRECOGNIZED,
                NotificationClassification.UNRECOGNIZED,
                "01N51",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.UNRECOGNIZED,
                                -1,
                                -1,
                                -1,
                                Map.of("relType", "Rel", "db", "neo4j"))
                        .asMap(),
                "warn: relationship type does not exist. The relationship type `Rel` does not exist in database `neo4j`. Verify that the spelling is correct.");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PROPERTY_NAME() {
        NotificationImplementation notification =
                missingPropertyName(InputPosition.empty, NotificationDetail.propertyName("prop"), "prop", "myDb");

        verifyNotification(
                notification,
                "The provided property key is not in the database",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownPropertyKeyWarning",
                "One of the property names in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application "
                        + "(the missing property name is: prop)",
                NotificationCategory.UNRECOGNIZED,
                NotificationClassification.UNRECOGNIZED,
                "01N52",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.UNRECOGNIZED,
                                -1,
                                -1,
                                -1,
                                Map.of("propKey", "prop", "db", "myDb"))
                        .asMap(),
                "warn: property key does not exist. The property `prop` does not exist in database `myDb`. Verify that the spelling is correct.");
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
                NotificationClassification.PERFORMANCE,
                "03N91",
                new DiagnosticRecord(info, NotificationClassification.PERFORMANCE, -1, -1, -1, Map.of("pat", pattern))
                        .asMap(),
                String.format(
                        "info: unbounded variable length pattern. The provided pattern '%s' is unbounded. Shortest path with an unbounded pattern may result in long execution times. Use an upper limit (e.g. '[*..5]') on the number of node hops in your pattern.",
                        pattern));
    }

    @Test
    void shouldConstructNotificationsFor_EXHAUSTIVE_SHORTEST_PATH() {
        NotificationImplementation notification = exhaustiveShortestPath(InputPosition.empty, List.of("length(p) > 1"));

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
                NotificationClassification.PERFORMANCE,
                "03N92",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.PERFORMANCE,
                                -1,
                                -1,
                                -1,
                                Map.of("predList", List.of("length(p) > 1")))
                        .asMap(),
                "info: exhaustive shortest path. The query runs with exhaustive shortest path due to the existential predicate(s) 'length(p) > 1'. It may be possible to use 'WITH' to separate the 'MATCH' from the existential predicate(s).");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PARAMETER_FOR_EXPLAIN() {
        NotificationImplementation notification = missingParameterForExplain(
                InputPosition.empty, NotificationDetail.missingParameters(List.of("param1")), List.of("param1"));

        verifyNotification(
                notification,
                "The statement refers to a parameter that was not provided in the request.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.ParameterNotProvided",
                "Did not supply query with enough parameters. "
                        + "The produced query plan will not be cached and is not executable without EXPLAIN. (Missing parameters: param1)",
                NotificationCategory.GENERIC,
                NotificationClassification.GENERIC,
                "01N60",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.GENERIC,
                                -1,
                                -1,
                                -1,
                                Map.of("paramList", List.of("param1")))
                        .asMap(),
                "warn: parameter missing. The query plan cannot be cached and is not executable without 'EXPLAIN' due to the undefined parameter(s) $`param1`. Provide the parameter(s).");
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PARAMETERS_FOR_EXPLAIN() {
        NotificationImplementation notification = missingParameterForExplain(
                InputPosition.empty,
                NotificationDetail.missingParameters(List.of("param1", "param2")),
                List.of("param1", "param2"));

        verifyNotification(
                notification,
                "The statement refers to a parameter that was not provided in the request.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.ParameterNotProvided",
                "Did not supply query with enough parameters. "
                        + "The produced query plan will not be cached and is not executable without EXPLAIN. (Missing parameters: param1, param2)",
                NotificationCategory.GENERIC,
                NotificationClassification.GENERIC,
                "01N60",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.GENERIC,
                                -1,
                                -1,
                                -1,
                                Map.of("paramList", List.of("param1", "param2")))
                        .asMap(),
                "warn: parameter missing. The query plan cannot be cached and is not executable without 'EXPLAIN' due to the undefined parameter(s) $`param1` and $`param2`. Provide the parameter(s).");
    }

    @Test
    void shouldConstructNotificationsFor_CODE_GENERATION_FAILED_expressionEngine() {
        String preparserOptions1 = "runtime=pipelined operatorEngine=compiled expressionEngine=compiled";
        String preparserOptions2 = "runtime=pipelined operatorEngine=compiled expressionEngine=interpreted";
        String failingEnginetype = "expression";
        String cause = "method too big";
        NotificationImplementation notification =
                codeGenerationFailed(InputPosition.empty, preparserOptions1, preparserOptions2, cause);

        verifyNotification(
                notification,
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.CodeGenerationFailed",
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log. (method too big)",
                NotificationCategory.PERFORMANCE,
                NotificationClassification.PERFORMANCE,
                "03N96",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.PERFORMANCE,
                                -1,
                                -1,
                                -1,
                                Map.of("cfgSetting", failingEnginetype, "cause", cause))
                        .asMap(),
                String.format(
                        "info: code generation failed. Failed to generate code, falling back to interpreted %s engine. A stacktrace can be found in the debug.log. Cause: %s.",
                        failingEnginetype, cause));
    }

    @Test
    void shouldConstructNotificationsFor_CODE_GENERATION_FAILED_operatorEngine() {
        String preparserOptions1 = "runtime=pipelined operatorEngine=compiled expressionEngine=compiled";
        String preparserOptions2 = "runtime=pipelined operatorEngine=interpreted expressionEngine=compiled";
        String failingEnginetype = "operator";
        String cause = "method too big";
        NotificationImplementation notification =
                codeGenerationFailed(InputPosition.empty, preparserOptions1, preparserOptions2, cause);

        verifyNotification(
                notification,
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.CodeGenerationFailed",
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log. (method too big)",
                NotificationCategory.PERFORMANCE,
                NotificationClassification.PERFORMANCE,
                "03N96",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.PERFORMANCE,
                                -1,
                                -1,
                                -1,
                                Map.of("cfgSetting", failingEnginetype, "cause", cause))
                        .asMap(),
                String.format(
                        "info: code generation failed. Failed to generate code, falling back to interpreted %s engine. A stacktrace can be found in the debug.log. Cause: %s.",
                        failingEnginetype, cause));
    }

    @Test
    void shouldConstructNotificationsFor_CODE_GENERATION_FAILED_defaultExpressionEngine() {
        // The failing engine is configured as the default (not the literal "compiled"), which is the
        // common case: the default expression engine still attempts compilation.
        String preparserOptions1 = "expressionEngine=default";
        String preparserOptions2 = "expressionEngine=interpreted";
        String failingEnginetype = "expression";
        String cause = "Failed to compile expression: ${regex:.*}";
        NotificationImplementation notification =
                codeGenerationFailed(InputPosition.empty, preparserOptions1, preparserOptions2, cause);

        verifyNotification(
                notification,
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.CodeGenerationFailed",
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log. ("
                        + cause + ")",
                NotificationCategory.PERFORMANCE,
                NotificationClassification.PERFORMANCE,
                "03N96",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.PERFORMANCE,
                                -1,
                                -1,
                                -1,
                                Map.of("cfgSetting", failingEnginetype, "cause", cause))
                        .asMap(),
                String.format(
                        "info: code generation failed. Failed to generate code, falling back to interpreted %s engine. A stacktrace can be found in the debug.log. Cause: %s.",
                        failingEnginetype, cause));
    }

    @Test
    void shouldConstructNotificationsFor_SUBQUERY_VARIABLE_SHADOWING() {
        NotificationImplementation notification =
                subqueryVariableShadowing(InputPosition.empty, NotificationDetail.shadowingVariable("v"), "CALL", "v");

        verifyNotification(
                notification,
                "Variable in subquery is shadowing a variable with the same name from the outer scope.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.SubqueryVariableShadowing",
                "Variable in subquery is shadowing a variable with the same name from the outer scope. "
                        + "If you want to use that variable instead, it must be imported into the subquery using "
                        + "a variable scope clause. (the shadowing variable is: v)",
                NotificationCategory.GENERIC,
                NotificationClassification.GENERIC,
                "03N60",
                new DiagnosticRecord(info, NotificationClassification.GENERIC, -1, -1, -1, Map.of("variable", "v"))
                        .asMap(),
                "info: subquery variable shadowing. The variable `v` in the subquery uses the same name as a variable from the outer query. Use 'CALL (`v`)' to import the one from the outer scope unless you want it to be a new variable.");
    }

    @Test
    void shouldConstructNotificationsFor_REDUNDANT_OPTIONAL_PROCEDURE() {
        NotificationImplementation notification = redundantOptionalProcedure(InputPosition.empty, "db.test");

        verifyNotification(
                notification,
                "The use of `OPTIONAL` is redundant when `CALL` is a void procedure.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.RedundantOptionalProcedure",
                "The use of `OPTIONAL` is redundant as `CALL db.test` is a void procedure.",
                NotificationCategory.GENERIC,
                NotificationClassification.GENERIC,
                "03N61",
                new DiagnosticRecord(info, NotificationClassification.GENERIC, -1, -1, -1, Map.of("proc", "db.test"))
                        .asMap(),
                "info: redundant optional procedure. The use of `OPTIONAL` is redundant as `CALL db.test()` is a void procedure.");
    }

    @Test
    void shouldConstructNotificationsFor_REDUNDANT_OPTIONAL_SUBQUERY() {
        NotificationImplementation notification = redundantOptionalSubquery(InputPosition.empty);

        verifyNotification(
                notification,
                "The use of `OPTIONAL` is redundant when `CALL` is a unit subquery.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.RedundantOptionalSubquery",
                "The use of `OPTIONAL` is redundant as `CALL` is a unit subquery.",
                NotificationCategory.GENERIC,
                NotificationClassification.GENERIC,
                "03N62",
                new DiagnosticRecord(info, NotificationClassification.GENERIC, -1, -1, -1, Map.of()).asMap(),
                "info: redundant optional subquery. The use of `OPTIONAL` is redundant as `CALL` is a unit subquery.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_IMPORTING_WITH_IN_SUBQUERY_CALL() {
        NotificationImplementation notification =
                deprecatedImportingWithInSubqueryCall(InputPosition.empty, "CALL", "a");

        String message = "CALL subquery without a variable scope clause is deprecated. Use CALL (a) { ... }";
        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "CALL subquery without a variable scope clause is deprecated. Use CALL (a) { ... }",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("item", message))
                        .asMap(),
                String.format("warn: feature deprecated. %s", message));
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_WHERE_VARIABLE_IN_NODE_PATTERN() {
        NotificationImplementation notification =
                deprecatedWhereVariableInNodePattern(InputPosition.empty, "Where", "{prop: 5}");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "'(Where {prop: 5})' is deprecated. It is replaced by '(`Where` {prop: 5})'.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "(Where {prop: 5})", "feat2", "(`Where` {prop: 5})"))
                        .asMap(),
                "warn: feature deprecated with replacement. (Where {prop: 5}) is deprecated. It is replaced by (`Where` {prop: 5}).");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_WHERE_VARIABLE_IN_RELATIONSHIP_PATTERN() {
        NotificationImplementation notification =
                deprecatedWhereVariableInRelationshipPattern(InputPosition.empty, "Where", "{prop: 5}");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "'-[Where {prop: 5}]-' is deprecated. It is replaced by '-[`Where` {prop: 5}]-'.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "-[Where {prop: 5}]-", "feat2", "-[`Where` {prop: 5}]-"))
                        .asMap(),
                "warn: feature deprecated with replacement. -[Where {prop: 5}]- is deprecated. It is replaced by -[`Where` {prop: 5}]-.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PRECEDENCE_OF_LABEL_EXPRESSION_PREDICATED() {
        NotificationImplementation notification =
                deprecatedPrecedenceOfLabelExpressionPredicate(InputPosition.empty, "n:A");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "'... + n:A' is deprecated. It is replaced by '... + (n:A)'.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "... + n:A", "feat2", "... + (n:A)"))
                        .asMap(),
                "warn: feature deprecated with replacement. ... + n:A is deprecated. It is replaced by ... + (n:A).");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_KEYWORD_VARIABLE_IN_WHEN_OPERAND() {
        NotificationImplementation notification =
                deprecatedKeywordVariableInWhenOperand(InputPosition.empty, "iS", " :: INTEGER");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "'WHEN iS :: INTEGER' is deprecated. It is replaced by 'WHEN `iS` :: INTEGER'.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "WHEN iS :: INTEGER", "feat2", "WHEN `iS` :: INTEGER"))
                        .asMap(),
                "warn: feature deprecated with replacement. WHEN iS :: INTEGER is deprecated. It is replaced by WHEN `iS` :: INTEGER.");
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
                NotificationClassification.UNRECOGNIZED,
                "00N50",
                new DiagnosticRecord(info, NotificationClassification.UNRECOGNIZED, -1, -1, -1, Map.of("db", "db"))
                        .asMap(),
                "note: successful completion - home database does not exist. The database `db` does not exist. Verify that the spelling is correct or create the database for the command to take effect.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_QUOTED_GRAPH_REFERENCE() {
        NotificationImplementation notification = deprecatedGraphReferenceNotification(
                "`alice's.composite`.alias", "`alice's.composite.alias`", InputPosition.empty);

        String message =
                "Graph references with separately backticked name parts (`alice's.composite`.alias) are deprecated. In future Cypher versions, use parameters or backtick the entire name (`alice's.composite.alias`).";
        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                message,
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("item", message))
                        .asMap(),
                String.format("warn: feature deprecated. %s".formatted(message), message));
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_QUOTED_GRAPH_BY_NAME_GRAPH_BY_NAME() {
        NotificationImplementation notification = deprecatedQuotedGraphByNameArgument(
                InputPosition.empty, "`alice's.composite`.alias", "alice's.composite.alias");

        String message =
                "Graph references with separately backticked name parts (`alice's.composite`.alias) are deprecated. In future Cypher versions, remove the backticks (alice's.composite.alias).";
        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                message,
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("item", message))
                        .asMap(),
                String.format("warn: feature deprecated. %s".formatted(message), message));
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
                NotificationClassification.GENERIC,
                "01N61",
                new DiagnosticRecord(warning, NotificationClassification.GENERIC, -1, -1, -1, Map.of("labelExpr", "!%"))
                        .asMap(),
                "warn: unsatisfiable relationship type expression. The expression '!%' cannot be satisfied because relationships must have exactly one type.");
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
                NotificationClassification.GENERIC,
                "01N63",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.GENERIC,
                                -1,
                                -1,
                                -1,
                                Map.of("variable", "r", "pat", "()-[r]->()<-[r]-()"))
                        .asMap(),
                "warn: repeated relationship pattern variable. `r` is repeated in '()-[r]->()<-[r]-()', which leads to no results.");
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
                NotificationClassification.GENERIC,
                "01N63",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.GENERIC,
                                -1,
                                -1,
                                -1,
                                Map.of("variable", "r", "pat", "()-[r*]->()-[r*]->()"))
                        .asMap(),
                "warn: repeated relationship pattern variable. `r` is repeated in '()-[r*]->()-[r*]->()', which leads to no results.");
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
                NotificationClassification.DEPRECATION,
                "01N02",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat", "connectComponentsPlanner"))
                        .asMap(),
                "warn: feature deprecated without replacement. connectComponentsPlanner is deprecated and will be removed without a replacement.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_EAGER_ANALYZER_PRE_PARSER_OPTION() {
        NotificationImplementation notification = deprecatedEagerAnalyzerPreParserOption(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The Cypher query option `eagerAnalyzer` is deprecated. "
                        + "It will be removed without a replacement. "
                        + "The option is ignored, eagerness analysis is systematically performed on the logical plan "
                        + "regardless of the value provided.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N02",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat", "eagerAnalyzer"))
                        .asMap(),
                "warn: feature deprecated without replacement. eagerAnalyzer is deprecated and will be removed without a replacement.");
    }

    @Test
    void shouldConstructNotificationsFor_RETIRED_PLANNER_VERSION_PRE_PARSER_OPTION() {
        NotificationImplementation notification = retiredPlannerVersionPreParserOption(InputPosition.empty, "v2026_03");

        verifyNotification(
                notification,
                "The requested planner version is no longer supported.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.PlannerVersionUnsupportedWarning",
                "The Cypher planner version v2026_03 is no longer supported. The default planner version is used instead.",
                NotificationCategory.UNSUPPORTED,
                NotificationClassification.UNSUPPORTED,
                "01N84",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.UNSUPPORTED,
                                -1,
                                -1,
                                -1,
                                Map.of("value", "v2026_03"))
                        .asMap(),
                "warn: unsupported planner version. The Cypher planner version v2026_03 is no longer supported. "
                        + "The default planner version is used instead.");
    }

    @Test
    void shouldConstructNotificationsFor_AUTH_PROVIDER_NOT_DEFINED() {
        NotificationImplementation notification = authProviderNotDefined(InputPosition.empty, "foo");

        verifyNotification(
                notification,
                "The auth provider is not defined.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Security.AuthProviderNotDefined",
                "The auth provider `foo` is not defined in the configuration. "
                        + "Verify that the spelling is correct or define `foo` in the configuration.",
                NotificationCategory.SECURITY,
                NotificationClassification.SECURITY,
                "00N72",
                new DiagnosticRecord(info, NotificationClassification.SECURITY, -1, -1, -1, Map.of("auth", "foo"))
                        .asMap(),
                "note: successful completion - undefined auth provider. "
                        + "The auth provider `foo` is not defined in the configuration. "
                        + "Verify that the spelling is correct or define `foo` in the configuration.");
    }

    @Test
    void shouldConstructNotificationsFor_EXTERNAL_AUTH_NOT_ENABLED() {
        NotificationImplementation notification = externalAuthNotEnabled(InputPosition.empty);

        verifyNotification(
                notification,
                "External auth for user is not enabled.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Security.ExternalAuthNotEnabled",
                "Use setting `dbms.security.require_local_user` to enable external auth.",
                NotificationCategory.SECURITY,
                NotificationClassification.SECURITY,
                "01N71",
                new DiagnosticRecord(warning, NotificationClassification.SECURITY, -1, -1, -1, Map.of()).asMap(),
                "warn: external auth disabled. Use the setting 'dbms.security.require_local_user' to enable external auth.");
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
                NotificationClassification.SECURITY,
                "00N70",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SECURITY,
                                -1,
                                -1,
                                -1,
                                Map.of("cmd", "GRANT WRITE ON GRAPH * TO editor"))
                        .asMap(),
                "note: successful completion - role or privilege already assigned. The command 'GRANT WRITE ON GRAPH * TO editor' has no effect. The role or privilege is already assigned.");
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
                NotificationClassification.SECURITY,
                "00N71",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SECURITY,
                                -1,
                                -1,
                                -1,
                                Map.of("cmd", "REVOKE ALL GRAPH PRIVILEGES ON GRAPH * FROM reader"))
                        .asMap(),
                "note: successful completion - role or privilege not assigned. The command 'REVOKE ALL GRAPH PRIVILEGES ON GRAPH * FROM reader' has no effect. The role or privilege is not assigned.");
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
                NotificationClassification.SECURITY,
                "00N70",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SECURITY,
                                -1,
                                -1,
                                -1,
                                Map.of("cmd", "GRANT ROLE aliceRole TO alice"))
                        .asMap(),
                "note: successful completion - role or privilege already assigned. The command 'GRANT ROLE aliceRole TO alice' has no effect. The role or privilege is already assigned.");
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
                NotificationClassification.SECURITY,
                "00N71",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SECURITY,
                                -1,
                                -1,
                                -1,
                                Map.of("cmd", "REVOKE ROLE other FROM alice"))
                        .asMap(),
                "note: successful completion - role or privilege not assigned. The command 'REVOKE ROLE other FROM alice' has no effect. The role or privilege is not assigned.");
    }

    @Test
    void shouldConstructNotificationsFor_COMMAND_HAS_NO_EFFECT_GRANT_ROLE_TO_AUTH_RULE() {
        NotificationImplementation notification =
                commandHasNoEffectGrantRoleToAuthRule(InputPosition.empty, "GRANT ROLE role TO AUTH RULE authRule");

        verifyNotification(
                notification,
                "`GRANT ROLE role TO AUTH RULE authRule` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Security.CommandHasNoEffect",
                "The auth rule already has the role. See Status Codes documentation for more information.",
                NotificationCategory.SECURITY,
                NotificationClassification.SECURITY,
                "00N70",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SECURITY,
                                -1,
                                -1,
                                -1,
                                Map.of("cmd", "GRANT ROLE role TO AUTH RULE authRule"))
                        .asMap(),
                "note: successful completion - role or privilege already assigned. The command 'GRANT ROLE role TO AUTH RULE authRule' has no effect. The role or privilege is already assigned.");
    }

    @Test
    void shouldConstructNotificationsFor_COMMAND_HAS_NO_EFFECT_REVOKE_ROLE_TO_AUTH_RULE() {
        NotificationImplementation notification =
                commandHasNoEffectRevokeRoleToAuthRule(InputPosition.empty, "REVOKE ROLE role FROM AUTH RULE authRule");

        verifyNotification(
                notification,
                "`REVOKE ROLE role FROM AUTH RULE authRule` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Security.CommandHasNoEffect",
                "The auth rule does not have the role. See Status Codes documentation for more information.",
                NotificationCategory.SECURITY,
                NotificationClassification.SECURITY,
                "00N71",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SECURITY,
                                -1,
                                -1,
                                -1,
                                Map.of("cmd", "REVOKE ROLE role FROM AUTH RULE authRule"))
                        .asMap(),
                "note: successful completion - role or privilege not assigned. The command 'REVOKE ROLE role FROM AUTH RULE authRule' has no effect. The role or privilege is not assigned.");
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
                NotificationClassification.SECURITY,
                "01N70",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.SECURITY,
                                -1,
                                -1,
                                -1,
                                Map.of("cmd", "REVOKE admina FROM ALICE", "msg", "Role does not exist."))
                        .asMap(),
                "warn: inoperational revoke command. The command 'REVOKE admina FROM ALICE' has no effect. Make sure nothing is misspelled. This notification will become an error in a future major version. Cause: Role does not exist.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROPERTY_REFERENCE_IN_CREATE() {
        NotificationImplementation notification = deprecatedPropertyReferenceInCreate(InputPosition.empty, "n.prop");

        String message =
                "Creating an entity (n.prop) and referencing that entity in a property definition in the same CREATE is deprecated.";
        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                message,
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("item", message))
                        .asMap(),
                String.format("warn: feature deprecated. %s", message));
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROPERTY_REFERENCE_IN_MERGE() {
        NotificationImplementation notification = deprecatedPropertyReferenceInMerge(InputPosition.empty, "n");

        String message =
                "Merging an entity (n) and referencing that entity in a property definition in the same MERGE is deprecated.";
        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                message,
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("item", message))
                        .asMap(),
                String.format("warn: feature deprecated. %s", message));
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
                NotificationClassification.TOPOLOGY,
                "00N80",
                new DiagnosticRecord(info, NotificationClassification.TOPOLOGY, -1, -1, -1, Map.of("server", "server"))
                        .asMap(),
                "note: successful completion - server already enabled. The command 'ENABLE SERVER' has no effect. Server 'server' is already enabled. Verify that this is the intended server.");
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
                NotificationClassification.TOPOLOGY,
                "00N81",
                new DiagnosticRecord(info, NotificationClassification.TOPOLOGY, -1, -1, -1, Map.of("server", "server"))
                        .asMap(),
                "note: successful completion - server already cordoned. The command 'CORDON SERVER' has no effect. The server 'server' is already cordoned. Verify that this is the intended server.");
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
                NotificationClassification.TOPOLOGY,
                "00N82",
                new DiagnosticRecord(info, NotificationClassification.TOPOLOGY, -1, -1, -1, Map.of()).asMap(),
                "note: successful completion - no databases reallocated. The command 'REALLOCATE DATABASES' has no effect. No databases were reallocated. No better allocation is currently possible.");
    }

    @Test
    void shouldConstructNotificationsFor_CORDONED_SERVERS_EXIST() {
        NotificationImplementation notification =
                cordonedServersExist(InputPosition.empty, List.of("server-1", "server-2", "server-3"));

        verifyNotification(
                notification,
                "Cordoned servers existed when making an allocation decision.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Cluster.CordonedServersExistedDuringAllocation",
                "Server(s) `server-1,server-2,server-3` are cordoned. This can impact allocation decisions.",
                NotificationCategory.TOPOLOGY,
                NotificationClassification.TOPOLOGY,
                "00N83",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.TOPOLOGY,
                                -1,
                                -1,
                                -1,
                                Map.of("serverList", List.of("server-1", "server-2", "server-3")))
                        .asMap(),
                "note: successful completion - cordoned servers existed during allocation. Cordoned servers existed when making an allocation decision. Server(s) 'server-1', 'server-2' and 'server-3' are cordoned. This can impact allocation decisions.");
    }

    @Test
    void shouldConstructNotificationsFor_REQUEST_TOPOLOGY_MATCHED_CURRENT_TOPOLOGY() {
        NotificationImplementation notification = requestedTopologyMatchedCurrentTopology(InputPosition.empty);

        verifyNotification(
                notification,
                "`ALTER DATABASE` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Cluster.RequestedTopologyMatchedCurrentTopology",
                "The requested topology matched the current topology. No allocations were changed.",
                NotificationCategory.TOPOLOGY,
                NotificationClassification.TOPOLOGY,
                "00N84",
                new DiagnosticRecord(info, NotificationClassification.TOPOLOGY, -1, -1, -1, Map.of()).asMap(),
                "note: successful completion - requested topology matched current topology. The command 'ALTER DATABASE' has no effect. The requested topology matched the current topology. No allocations were changed.");
    }

    @Test
    void shouldConstructNotificationsFor_INDEX_OR_CONSTRAINT_ALREADY_EXISTS() {
        NotificationImplementation notification = indexOrConstraintAlreadyExists(
                InputPosition.empty,
                "CREATE INDEX foo IF NOT EXISTS FOR (n:L) ON (n.p1)",
                "INDEX foo FOR (n:L) ON (n.p2)");

        verifyNotification(
                notification,
                "`CREATE INDEX foo IF NOT EXISTS FOR (n:L) ON (n.p1)` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Schema.IndexOrConstraintAlreadyExists",
                "`INDEX foo FOR (n:L) ON (n.p2)` already exists.",
                NotificationCategory.SCHEMA,
                NotificationClassification.SCHEMA,
                "00NA0",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SCHEMA,
                                -1,
                                -1,
                                -1,
                                Map.of(
                                        "cmd",
                                        "CREATE INDEX foo IF NOT EXISTS FOR (n:L) ON (n.p1)",
                                        "idxOrConstrPat",
                                        "INDEX foo FOR (n:L) ON (n.p2)"))
                        .asMap(),
                "note: successful completion - index or constraint already exists. The command 'CREATE INDEX foo IF NOT EXISTS FOR (n:L) ON (n.p1)' has no effect. The index or constraint specified by 'INDEX foo FOR (n:L) ON (n.p2)' already exists.");

        notification = indexOrConstraintAlreadyExists(
                InputPosition.empty,
                "CREATE CONSTRAINT bar IF NOT EXISTS FOR (n:L) REQUIRE (n.p1) IS NODE KEY",
                "CONSTRAINT baz FOR (n:L) REQUIRE (n.p1) IS NODE KEY");

        verifyNotification(
                notification,
                "`CREATE CONSTRAINT bar IF NOT EXISTS FOR (n:L) REQUIRE (n.p1) IS NODE KEY` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Schema.IndexOrConstraintAlreadyExists",
                "`CONSTRAINT baz FOR (n:L) REQUIRE (n.p1) IS NODE KEY` already exists.",
                NotificationCategory.SCHEMA,
                NotificationClassification.SCHEMA,
                "00NA0",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SCHEMA,
                                -1,
                                -1,
                                -1,
                                Map.of(
                                        "cmd",
                                        "CREATE CONSTRAINT bar IF NOT EXISTS FOR (n:L) REQUIRE (n.p1) IS NODE KEY",
                                        "idxOrConstrPat",
                                        "CONSTRAINT baz FOR (n:L) REQUIRE (n.p1) IS NODE KEY"))
                        .asMap(),
                "note: successful completion - index or constraint already exists. The command 'CREATE CONSTRAINT bar IF NOT EXISTS FOR (n:L) REQUIRE (n.p1) IS NODE KEY' has no effect. The index or constraint specified by 'CONSTRAINT baz FOR (n:L) REQUIRE (n.p1) IS NODE KEY' already exists.");

        notification = indexOrConstraintAlreadyExists(
                InputPosition.empty,
                "CREATE CONSTRAINT bar IF NOT EXISTS FOR ()-[r:R]->() REQUIRE (r.p1) IS KEY",
                "CONSTRAINT baz FOR ()-[r:R]->() REQUIRE (r.p1) IS KEY");

        verifyNotification(
                notification,
                "`CREATE CONSTRAINT bar IF NOT EXISTS FOR ()-[r:R]->() REQUIRE (r.p1) IS KEY` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Schema.IndexOrConstraintAlreadyExists",
                "`CONSTRAINT baz FOR ()-[r:R]->() REQUIRE (r.p1) IS KEY` already exists.",
                NotificationCategory.SCHEMA,
                NotificationClassification.SCHEMA,
                "00NA0",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SCHEMA,
                                -1,
                                -1,
                                -1,
                                Map.of(
                                        "cmd",
                                        "CREATE CONSTRAINT bar IF NOT EXISTS FOR ()-[r:R]->() REQUIRE (r.p1) IS KEY",
                                        "idxOrConstrPat",
                                        "CONSTRAINT baz FOR ()-[r:R]->() REQUIRE (r.p1) IS KEY"))
                        .asMap(),
                "note: successful completion - index or constraint already exists. The command 'CREATE CONSTRAINT bar IF NOT EXISTS FOR ()-[r:R]->() REQUIRE (r.p1) IS KEY' has no effect. The index or constraint specified by 'CONSTRAINT baz FOR ()-[r:R]->() REQUIRE (r.p1) IS KEY' already exists.");
    }

    @Test
    void shouldConstructNotificationsFor_INDEX_OR_CONSTRAINT_DOES_NOT_EXIST() {
        NotificationImplementation notification =
                indexOrConstraintDoesNotExist(InputPosition.empty, "DROP INDEX foo IF EXISTS", "foo");

        verifyNotification(
                notification,
                "`DROP INDEX foo IF EXISTS` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Schema.IndexOrConstraintDoesNotExist",
                "`foo` does not exist.",
                NotificationCategory.SCHEMA,
                NotificationClassification.SCHEMA,
                "00NA1",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SCHEMA,
                                -1,
                                -1,
                                -1,
                                Map.of("cmd", "DROP INDEX foo IF EXISTS", "idxOrConstr", "foo"))
                        .asMap(),
                "note: successful completion - index or constraint does not exist. The command 'DROP INDEX foo IF EXISTS' has no effect. The specified index or constraint `foo` does not exist.");

        notification = indexOrConstraintDoesNotExist(InputPosition.empty, "DROP CONSTRAINT bar IF EXISTS", "foo");

        verifyNotification(
                notification,
                "`DROP CONSTRAINT bar IF EXISTS` has no effect.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Schema.IndexOrConstraintDoesNotExist",
                "`foo` does not exist.",
                NotificationCategory.SCHEMA,
                NotificationClassification.SCHEMA,
                "00NA1",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.SCHEMA,
                                -1,
                                -1,
                                -1,
                                Map.of("cmd", "DROP CONSTRAINT bar IF EXISTS", "idxOrConstr", "foo"))
                        .asMap(),
                "note: successful completion - index or constraint does not exist. The command 'DROP CONSTRAINT bar IF EXISTS' has no effect. The specified index or constraint `foo` does not exist.");
    }

    @Test
    void shouldConstructNotificationsFor_VECTOR_INDEX_DIMENSIONS_NOT_SPECIFIED() {
        NotificationImplementation notification = vectorIndexDimensionsNotSpecified(InputPosition.empty);

        verifyNotification(
                notification,
                "Vector index dimensions not specified.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Schema.VectorIndexDimensionsNotSpecified",
                "When creating a vector index, `vector.dimensions` should be specified. Omitting it is allowed, but specifying dimensions ensures that only vectors of that size are indexed and makes dimension mismatches fail clearly at query time. For example, set `OPTIONS { indexConfig: { `vector.dimensions`: 1536 } }` when creating the index.",
                NotificationCategory.SCHEMA,
                NotificationClassification.SCHEMA,
                "00NA2",
                new DiagnosticRecord(info, NotificationClassification.SCHEMA, -1, -1, -1).asMap(),
                "note: successful completion - vector index dimensions not specified. When creating a vector index, `vector.dimensions` should be specified. Omitting it is allowed, but specifying dimensions ensures that only vectors of that size are indexed and makes dimension mismatches fail clearly at query time. For example, set `OPTIONS { indexConfig: { `vector.dimensions`: 1536 } }` when creating the index.");
    }

    @Test
    void shouldKeep_VECTOR_INDEX_DIMENSIONS_NOT_SPECIFIED_description_in_sync_with_gql_status() {
        assertThat(NotificationCodeWithDescription.VECTOR_INDEX_DIMENSIONS_NOT_SPECIFIED.getDescription(new Object[0]))
                .isEqualTo(GqlStatusInfoCodes.STATUS_00NA2.getMessage(new Object[0]));
    }

    @Test
    void shouldConstructNotificationsFor_AGGREGATION_SKIPPED_NULL() {
        NotificationImplementation notification = aggregationSkippedNull();

        verifyNotification(
                notification,
                "The query contains an aggregation function that skips null values.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.AggregationSkippedNull",
                "null value eliminated in set function.",
                NotificationCategory.UNRECOGNIZED,
                NotificationClassification.UNRECOGNIZED,
                "01G11",
                new DiagnosticRecord(warning, NotificationClassification.UNRECOGNIZED, -1, -1, -1, Map.of()).asMap(),
                "warn: null value eliminated in set function");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_BOOLEAN_COERCION() {
        NotificationImplementation notification = deprecatedBooleanCoercion();

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query converted a list or path to a boolean value.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N02",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat", "Converting a list or a path to a boolean"))
                        .asMap(),
                "warn: feature deprecated without replacement. Converting a list or a path to a boolean is deprecated and will be removed without a replacement.");
    }

    @Test
    void shouldConstructNotificationsFor_INSECURE_PROTOCOL() {
        NotificationImplementation notification = insecureProtocol();

        verifyNotification(
                notification,
                "The query uses an insecure protocol. Please consider using 'https' instead.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.InsecureProtocol",
                "Query uses an insecure protocol.",
                NotificationCategory.SECURITY,
                NotificationClassification.SECURITY,
                "01N72",
                new DiagnosticRecord(warning, NotificationClassification.SECURITY, -1, -1, -1, Map.of()).asMap(),
                "warn: insecure URL protocol. Query uses an insecure protocol. Consider using 'https' instead.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_OPTION_IN_OPTION_MAP() {
        NotificationImplementation notification = deprecatedOptionInOptionMap("oldName", "newName");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "`oldName` is deprecated. It is replaced by `newName`.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N01",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "oldName", "feat2", "newName"))
                        .asMap(),
                "warn: feature deprecated with replacement. oldName is deprecated. It is replaced by newName.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_SEEDING_OPTION() {
        NotificationImplementation notification = deprecatedSeedingOption("oldName");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "`oldName` is deprecated. Credentials are now supplied via the cloud provider mechanisms.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N02",
                new DiagnosticRecord(
                                warning, NotificationClassification.DEPRECATION, -1, -1, -1, Map.of("feat1", "oldName"))
                        .asMap(),
                "warn: feature deprecated without replacement. oldName is deprecated and will be removed without a replacement.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_EXISTING_DATA_OPTION() {
        NotificationImplementation notification = deprecatedExistingDataOption();

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "`existingData` is deprecated. Use of existing data is implicit with seeding.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N02",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of("feat1", "existingData"))
                        .asMap(),
                "warn: feature deprecated without replacement. existingData is deprecated and will be removed without a replacement.");
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_STORE_FORMAT() {
        NotificationImplementation notification = deprecatedStoreFormat("oldFormat");

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The targeted store format: oldFormat is deprecated. For details on deprecated store formats, see https://neo4j.com/docs/store-format-deprecations.",
                NotificationCategory.DEPRECATION,
                NotificationClassification.DEPRECATION,
                "01N00",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.DEPRECATION,
                                -1,
                                -1,
                                -1,
                                Map.of(
                                        "item",
                                        "The targeted store format: oldFormat is deprecated. For details on deprecated store formats, see https://neo4j.com/docs/store-format-deprecations."))
                        .asMap(),
                "warn: feature deprecated. The targeted store format: oldFormat is deprecated. For details on deprecated store formats, see https://neo4j.com/docs/store-format-deprecations.");
    }

    @Test
    void shouldConstructNotificationsFor_WAIT_SERVER_UNAVAILABLE() {
        NotificationImplementation notification = waitServerUnavailable("serverName");

        verifyNotification(
                notification,
                "Server is not available.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Cluster.ServerNotAvailable",
                "Server `serverName` is not available.",
                NotificationCategory.TOPOLOGY,
                NotificationClassification.TOPOLOGY,
                "01N82",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.TOPOLOGY,
                                -1,
                                -1,
                                -1,
                                Map.of("item", "Server `serverName` is not available."))
                        .asMap(),
                "warn: server is not available. Server `'serverName'` is not available.");
    }

    @Test
    void shouldConstructNotificationsFor_WAIT_SERVER_CATCHING_UP() {
        NotificationImplementation notification = waitServerCatchingUp("serverName", "localhost:1234");

        verifyNotification(
                notification,
                "Server is still catching up.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Cluster.ServerCatchingUp",
                "Server `serverName` at address `localhost:1234` is still catching up.",
                NotificationCategory.TOPOLOGY,
                NotificationClassification.TOPOLOGY,
                "01N81",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.TOPOLOGY,
                                -1,
                                -1,
                                -1,
                                Map.of("item", "Server `serverName` at address `'localhost:1234'` is catching up."))
                        .asMap(),
                "warn: server is catching up. Server `'serverName'` at address `'localhost:1234'` is still catching up.");
    }

    @Test
    void shouldConstructNotificationsFor_WAIT_SERVER_FAILED() {
        NotificationImplementation notification =
                waitServerFailed("serverName", "localhost:1234", "Server failed because foo.");

        verifyNotification(
                notification,
                "Server failed.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Cluster.ServerFailed",
                "Server `serverName` at address `localhost:1234` failed: Server failed because foo.",
                NotificationCategory.TOPOLOGY,
                NotificationClassification.TOPOLOGY,
                "01N80",
                new DiagnosticRecord(
                                warning,
                                NotificationClassification.TOPOLOGY,
                                -1,
                                -1,
                                -1,
                                Map.of(
                                        "item",
                                        "Server `serverName` at address `'localhost:1234'` failed: Server failed because foo."))
                        .asMap(),
                "warn: server failed. Server `'serverName'` at address `'localhost:1234'` failed: Server failed because foo.");
    }

    @Test
    void shouldConstructNotificationsFor_WAIT_SERVER_CAUGHT_UP() {
        NotificationImplementation notification = waitServerCaughtUp("serverName", "localhost:1234");

        verifyNotification(
                notification,
                "Server has caught up.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Cluster.ServerCaughtUp",
                "Server `serverName` at address `localhost:1234` has caught up.",
                NotificationCategory.TOPOLOGY,
                NotificationClassification.TOPOLOGY,
                "03N85",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.TOPOLOGY,
                                -1,
                                -1,
                                -1,
                                Map.of("item", "Server `serverName` at address `'localhost:1234'` has caught up."))
                        .asMap(),
                "info: server has caught up. Server `'serverName'` at address `'localhost:1234'` has caught up.");
    }

    @Test
    void shouldConstructNotificationsFor_CALLABLE_SHADOWING() {
        NotificationImplementation notification = callableShadowing(InputPosition.empty, "kind", "callableName");

        verifyNotification(
                notification,
                "A callable is shadowing another callable in scope.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.CallableShadowing",
                "Local kind `callableName` shadows a built-in or external kind with the same name.",
                NotificationCategory.GENERIC,
                NotificationClassification.GENERIC,
                "03N64",
                new DiagnosticRecord(
                                info,
                                NotificationClassification.GENERIC,
                                -1,
                                -1,
                                -1,
                                Map.of(
                                        "item",
                                        "Local kind `callableName` shadows a built-in or external kind with the same name."))
                        .asMap(),
                "info: callable shadowing. Local kind `callableName` shadows a built-in or external kind with the same name.");
    }

    @Test
    void shouldConstructNotificationsFor_IDENTIFIER_SHADOWING_VARIABLE() {
        NotificationImplementation notification =
                identifierShadowingVariable(InputPosition.empty, "indexName", "VECTOR INDEX");

        verifyNotification(
                notification,
                "An identifier is shadowing a variable in scope.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.IdentifierShadowingVariable",
                "The identifier `indexName` in the `VECTOR INDEX` clause has the same name as a variable in scope. "
                        + "Regardless of what the variable evaluates to, it is the literal `indexName` that will be used.",
                NotificationCategory.GENERIC,
                NotificationClassification.GENERIC,
                "03N63",
                new DiagnosticRecord(info, NotificationClassification.GENERIC, -1, -1, -1).asMap(),
                "info: identifier shadowing variable. "
                        + "The identifier `indexName` in the VECTOR INDEX clause has the same name as a variable in scope. "
                        + "Regardless of what the variable evaluates to, it is the literal `indexName` that will be used.");
    }

    @Test
    void shouldConstructNotificationsFor_VIRTUAL_GRAPH_POST_PROCESSING() {
        final var notification = NotificationCodeWithDescription.graphEngineFallbackPostProcessing();

        verifyNotification(
                notification,
                "The query plan against a virtual graph contains a potentially expensive post-processing step.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.VirtualGraphPostProcessing",
                "The query execution plan contains a post-processing step that materialize intermediate results. This may transfer large amounts of data from the remote source and increase memory usage. Consider rewriting the query so that aggregation, `ORDER BY`, `DISTINCT`, and `LIMIT` can be pushed down to the remote source.",
                NotificationCategory.PERFORMANCE,
                NotificationClassification.PERFORMANCE,
                "03N97",
                new DiagnosticRecord(info, NotificationClassification.PERFORMANCE, -1, -1, -1).asMap(),
                "info: virtual graph post-processing. The query execution plan contains a post-processing step that materialize intermediate results. This may transfer large amounts of data from the remote source and increase memory usage. Consider rewriting the query so that aggregation, `ORDER BY`, `DISTINCT`, and `LIMIT` can be pushed down to the remote source.");
    }

    private void verifyNotification(
            NotificationImplementation notification,
            String title,
            SeverityLevel severity,
            String code,
            String description,
            NotificationCategory category,
            NotificationClassification classification,
            String gqlStatusString,
            Map<String, Object> diagnosticRecord,
            String statusDescription) {
        assertThat(notification.getTitle()).isEqualTo(title);
        assertThat(notification.getSeverity()).isEqualTo(severity);
        assertThat(notification.getCode()).isEqualTo(code);
        assertThat(notification.getDescription()).isEqualTo(description);
        assertThat(notification.getCategory()).isEqualTo(category);
        assertThat(notification.getClassification()).isEqualTo(classification);
        assertThat(notification.gqlStatus()).isEqualTo(gqlStatusString);
        assertThat(notification.diagnosticRecord()).isEqualTo(diagnosticRecord);
        assertThat(notification.statusDescription()).isEqualTo(statusDescription);
    }

    private final String warning = SeverityLevel.WARNING.name();
    private final String info = SeverityLevel.INFORMATION.name();

    @Test
    void allNotificationsShouldBeAClientNotification() {

        stream(NotificationCodeWithDescription.values())
                .forEach(notification ->
                        assertThat(notification.getStatus().code().serialize()).contains("ClientNotification"));
    }

    @Test
    void noNotificationShouldHaveUnknownCategory() {
        stream(NotificationCodeWithDescription.values())
                .forEach(notification -> assertThat(((Status.NotificationCode)
                                        notification.getStatus().code())
                                .getNotificationCategory())
                        .isNotEqualTo(NotificationCategory.UNKNOWN.name()));
    }

    @Test
    void noNotificationShouldHaveUnknownClassification() {
        stream(NotificationCodeWithDescription.values()).forEach(notification -> {
            var notificationImpl = new NotificationImplementation.NotificationBuilder(notification)
                    // We need to fake a message parameter value array here
                    // To make sure it has the correct length we set it to the message parameter key array
                    .setMessageParameters(((GqlStatusInfoCodes) notification.getGqlStatusInfo())
                            .getStatusParameterKeys().stream()
                                    .map(GqlParams.GqlParam::name)
                                    .toArray())
                    .build();
            assertThat(notificationImpl.getClassification()).isNotEqualTo(NotificationClassification.UNKNOWN);
        });
    }

    @Test
    void notificationsShouldNotHaveTooFewParameterValues() {
        var notificationBuilder =
                new NotificationImplementation.NotificationBuilder(NotificationCodeWithDescription.MISSING_REL_TYPE);

        assertThatThrownBy(() -> notificationBuilder
                        .setMessageParameters(new String[] {})
                        .build())
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Expected parameterKeys: [relType, db] and parameterValues: [] to have the same length.");
    }

    @Test
    void notificationsShouldNotHaveTooManyParameterValues() {
        var notificationBuilder =
                new NotificationImplementation.NotificationBuilder(NotificationCodeWithDescription.MISSING_REL_TYPE);

        assertThatThrownBy(() -> notificationBuilder
                        .setMessageParameters(new String[] {"A", "B", "C"})
                        .build())
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Expected parameterKeys: [relType, db] and parameterValues: [A, B, C] to have the same length.");
    }

    /**
     * If this test fails, you have added, changed or removed a notification.
     * To get it approved, follow the instructions on
     * https://linear.app/neo4j/team/SURF/new?template=fb219e16-d9e6-4105-86b1-7dffb7442292
     * When your changes have been approved, please change the expected byte[] below.
     */
    @Test
    void verifyNotificationsHaveNotChanged() {
        // Hack to not have to break this test now that the template string is not available.
        final var args = Stream.generate(() -> "%s").limit(64).toArray();
        StringBuilder notificationBuilder = new StringBuilder();
        stream(NotificationCodeWithDescription.values()).forEach(notification -> {
            var status = notification.getStatus();
            Status.NotificationCode notificationCode = (Status.NotificationCode) status.code();

            // Covers all notification information except NotificationDetail and position, which are query dependent
            notificationBuilder.append(notificationCode.description()); // Title
            notificationBuilder.append(notification.getDescription(args)); // Description
            notificationBuilder.append(notification.getGqlStatusInfo());
            notificationBuilder.append(notification.getGqlStatusInfo().getMessage(new Object[0]));
            notificationBuilder.append(notificationCode.serialize());
            notificationBuilder.append(notificationCode.getSeverity());
            notificationBuilder.append(notificationCode.getNotificationCategory());
        });

        byte[] notificationHash = DigestUtils.sha256(notificationBuilder.toString());

        byte[] expectedHash = new byte[] {
            19, 119, 23, -22, 90, -18, 7, -113, 64, 31, 65, 4, -109, 21, 122, -67, 99, 17, 123, -104, 56, -44, 115, -86,
            55, 116, -86, 112, 61, -89, 72, -93
        };

        if (!Arrays.equals(notificationHash, expectedHash)) {
            fail(
                    "Expected: " + Arrays.toString(expectedHash) + " \n Actual: " + Arrays.toString(notificationHash)
                            + "\n If you have added, changed or removed a notification, "
                            + "please follow the process on https://linear.app/neo4j/team/SURF/new?template=fb219e16-d9e6-4105-86b1-7dffb7442292");
        }
    }
}
