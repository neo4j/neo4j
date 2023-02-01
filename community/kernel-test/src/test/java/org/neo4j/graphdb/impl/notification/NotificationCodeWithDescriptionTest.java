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

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.SeverityLevel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.CARTESIAN_PRODUCT;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.CODE_GENERATION_FAILED;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.DEPRECATED_DATABASE_NAME;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.DEPRECATED_FORMAT;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.DEPRECATED_FUNCTION;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.DEPRECATED_PROCEDURE;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.DEPRECATED_PROCEDURE_RETURN_FIELD;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.DEPRECATED_RUNTIME_OPTION;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.EAGER_LOAD_CSV;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.EXHAUSTIVE_SHORTEST_PATH;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.HOME_DATABASE_NOT_PRESENT;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.INDEX_HINT_UNFULFILLABLE;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.JOIN_HINT_UNFULFILLABLE;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.LARGE_LABEL_LOAD_CSV;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.MISSING_LABEL;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.MISSING_PARAMETERS_FOR_EXPLAIN;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.MISSING_PROPERTY_NAME;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.MISSING_REL_TYPE;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.PROCEDURE_WARNING;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.REPEATED_RELATIONSHIP_REFERENCE;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.RUNTIME_EXPERIMENTAL;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.RUNTIME_UNSUPPORTED;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.SUBQUERY_VARIABLE_SHADOWING;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.UNBOUNDED_SHORTEST_PATH;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.UNION_RETURN_ORDER;
import static org.neo4j.graphdb.impl.notification.NotificationCodeWithDescription.UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION;
import static org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.repeatedRelationship;
import static org.neo4j.graphdb.impl.notification.NotificationDetail.Factory.unsatisfiableRelTypeExpression;

class NotificationCodeWithDescriptionTest {
    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_node_index() {
        NotificationDetail indexDetail = NotificationDetail.Factory.nodeAnyIndex("person", "Person", "name");
        Notification notification = INDEX_HINT_UNFULFILLABLE.notification(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: INDEX FOR (`person`:`Person`) ON (`person`.`name`))",
                NotificationCategory.HINT);
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_node_btree_index() {
        NotificationDetail indexDetail = NotificationDetail.Factory.nodeBtreeIndex("person", "Person", "name");
        Notification notification = INDEX_HINT_UNFULFILLABLE.notification(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: BTREE INDEX FOR (`person`:`Person`) ON (`person`.`name`))",
                NotificationCategory.HINT);
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_node_text_index() {
        NotificationDetail indexDetail = NotificationDetail.Factory.nodeTextIndex("person", "Person", "name");
        Notification notification = INDEX_HINT_UNFULFILLABLE.notification(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: TEXT INDEX FOR (`person`:`Person`) ON (`person`.`name`))",
                NotificationCategory.HINT);
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_rel_index() {
        NotificationDetail indexDetail = NotificationDetail.Factory.relationshipAnyIndex("person", "Person", "name");
        Notification notification = INDEX_HINT_UNFULFILLABLE.notification(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`))",
                NotificationCategory.HINT);
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_rel_btree_index() {
        NotificationDetail indexDetail = NotificationDetail.Factory.relationshipBtreeIndex("person", "Person", "name");
        Notification notification = INDEX_HINT_UNFULFILLABLE.notification(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: BTREE INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`))",
                NotificationCategory.HINT);
    }

    @Test
    void shouldConstructNotificationFor_INDEX_HINT_UNFULFILLABLE_for_rel_text_index() {
        NotificationDetail indexDetail = NotificationDetail.Factory.relationshipTextIndex("person", "Person", "name");
        Notification notification = INDEX_HINT_UNFULFILLABLE.notification(InputPosition.empty, indexDetail);

        verifyNotification(
                notification,
                "The request (directly or indirectly) referred to an index that does not exist.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Schema.HintedIndexNotFound",
                "The hinted index does not exist, please check the schema (index is: TEXT INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`))",
                NotificationCategory.HINT);
    }

    @Test
    void shouldConstructNotificationFor_CARTESIAN_PRODUCT() {
        Set<String> idents = new TreeSet<>();
        idents.add("n");
        idents.add("node2");
        NotificationDetail identifierDetail = NotificationDetail.Factory.cartesianProduct(idents);
        Notification notification = CARTESIAN_PRODUCT.notification(InputPosition.empty, identifierDetail);

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
                NotificationCategory.PERFORMANCE);
    }

    @Test
    void shouldConstructNotificationsFor_JOIN_HINT_UNFULFILLABLE() {
        List<String> idents = new ArrayList<>();
        idents.add("n");
        idents.add("node2");
        NotificationDetail identifierDetail = NotificationDetail.Factory.joinKey(idents);
        Notification notification = JOIN_HINT_UNFULFILLABLE.notification(InputPosition.empty, identifierDetail);

        verifyNotification(
                notification,
                "The database was unable to plan a hinted join.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.JoinHintUnfulfillableWarning",
                "The hinted join was not planned. This could happen because no generated plan contained the join key, "
                        + "please try using a different join key or restructure your query. "
                        + "(hinted join key identifiers are: n, node2)",
                NotificationCategory.HINT);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE() {
        NotificationDetail identifierDetail = NotificationDetail.Factory.deprecatedName("oldName", "newName");
        Notification notification = DEPRECATED_PROCEDURE.notification(InputPosition.empty, identifierDetail);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated procedure. ('oldName' has been replaced by 'newName')",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE_with_no_newName() {
        NotificationDetail identifierDetail = NotificationDetail.Factory.deprecatedName("oldName", "");
        Notification notification = DEPRECATED_PROCEDURE.notification(InputPosition.empty, identifierDetail);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated procedure. ('oldName' is no longer supported)",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_RUNTIME_UNSUPPORTED() {
        Notification notification = RUNTIME_UNSUPPORTED.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This query is not supported by the chosen runtime.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RuntimeUnsupportedWarning",
                "Selected runtime is unsupported for this query, please use a different runtime instead or fallback to default.",
                NotificationCategory.UNSUPPORTED);
    }

    @Test
    void shouldConstructNotificationsFor_INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY() {
        Notification notification = INDEX_LOOKUP_FOR_DYNAMIC_PROPERTY.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "Queries using dynamic properties will use neither index seeks nor index scans for those properties",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.DynamicProperty",
                "Using a dynamic property makes it impossible to use an index lookup for this query",
                NotificationCategory.PERFORMANCE);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_FUNCTION() {
        Notification notification = DEPRECATED_FUNCTION.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated function.",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_RUNTIME_OPTION() {
        Notification notification = DEPRECATED_RUNTIME_OPTION.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated runtime option.",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_PROCEDURE_WARNING() {
        Notification notification = PROCEDURE_WARNING.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "The query used a procedure that generated a warning.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Procedure.ProcedureWarning",
                "The query used a procedure that generated a warning.",
                NotificationCategory.GENERIC);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_PROCEDURE_RETURN_FIELD() {
        Notification notification = DEPRECATED_PROCEDURE_RETURN_FIELD.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The query used a deprecated field from a procedure.",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR() {
        Notification notification = DEPRECATED_RELATIONSHIP_TYPE_SEPARATOR.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The semantics of using colon in the separation of alternative relationship types will change in a future version.",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE() {
        Notification notification = DEPRECATED_NODE_OR_RELATIONSHIP_ON_RHS_SET_CLAUSE.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The use of nodes or relationships for setting properties is deprecated and will be removed in a future version. "
                        + "Please use properties() instead.",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP() {
        Notification notification =
                DEPRECATED_SHORTEST_PATH_WITH_FIXED_LENGTH_RELATIONSHIP.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "The use of shortestPath and allShortestPaths with fixed length relationships is deprecated and will be removed in a future version. "
                        + "Please use a path with a length of 1 [r*1..1] instead or a Match with a limit.",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_EAGER_LOAD_CSV() {
        Notification notification = EAGER_LOAD_CSV.notification(InputPosition.empty);

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
                NotificationCategory.PERFORMANCE);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_FORMAT() {
        Notification notification = DEPRECATED_FORMAT.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "The client made a request for a format which has been deprecated.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Request.DeprecatedFormat",
                "The requested format has been deprecated.",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_LARGE_LABEL_LOAD_CSV() {
        Notification notification = LARGE_LABEL_LOAD_CSV.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "Adding a schema index may speed up this query.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.NoApplicableIndex",
                "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely "
                        + "not perform well on large data sets. Please consider using a schema index.",
                NotificationCategory.PERFORMANCE);
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_LABEL() {
        Notification notification = MISSING_LABEL.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "The provided label is not in the database.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownLabelWarning",
                "One of the labels in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application",
                NotificationCategory.UNRECOGNIZED);
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_REL_TYPE() {
        Notification notification = MISSING_REL_TYPE.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "The provided relationship type is not in the database.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownRelationshipTypeWarning",
                "One of the relationship types in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application",
                NotificationCategory.UNRECOGNIZED);
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PROPERTY_NAME() {
        Notification notification = MISSING_PROPERTY_NAME.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "The provided property key is not in the database",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnknownPropertyKeyWarning",
                "One of the property names in your query is not available in the database, make sure you didn't "
                        + "misspell it or that the label is available when you run this statement in your application",
                NotificationCategory.UNRECOGNIZED);
    }

    @Test
    void shouldConstructNotificationsFor_UNBOUNDED_SHORTEST_PATH() {
        Notification notification = UNBOUNDED_SHORTEST_PATH.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "The provided pattern is unbounded, consider adding an upper limit to the number of node hops.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.UnboundedVariableLengthPattern",
                "Using shortest path with an unbounded pattern will likely result in long execution times. "
                        + "It is recommended to use an upper limit to the number of node hops in your pattern.",
                NotificationCategory.PERFORMANCE);
    }

    @Test
    void shouldConstructNotificationsFor_EXHAUSTIVE_SHORTEST_PATH() {
        Notification notification = EXHAUSTIVE_SHORTEST_PATH.notification(InputPosition.empty);

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
                NotificationCategory.PERFORMANCE);
    }

    @Test
    void shouldConstructNotificationsFor_RUNTIME_EXPERIMENTAL() {
        Notification notification = RUNTIME_EXPERIMENTAL.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is experimental and should not be used in production systems.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RuntimeExperimental",
                "You are using an experimental feature",
                NotificationCategory.UNSUPPORTED);
    }

    @Test
    void shouldConstructNotificationsFor_MISSING_PARAMETERS_FOR_EXPLAIN() {
        Notification notification = MISSING_PARAMETERS_FOR_EXPLAIN.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "The statement refers to a parameter that was not provided in the request.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.ParameterNotProvided",
                "Did not supply query with enough parameters. The produced query plan will not be cached and is not executable without EXPLAIN.",
                NotificationCategory.GENERIC);
    }

    @Test
    void shouldConstructNotificationsFor_SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY() {
        Notification notification = SUBOPTIMAL_INDEX_FOR_CONTAINS_QUERY.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "Index cannot execute wildcard query efficiently",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.SuboptimalIndexForWildcardQuery",
                "If the performance of this statement using `CONTAINS` doesn't meet your expectations check out the alternative index-providers, see "
                        + "documentation on index configuration.",
                NotificationCategory.PERFORMANCE);
    }

    @Test
    void shouldConstructNotificationsFor_SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY() {
        Notification notification = SUBOPTIMAL_INDEX_FOR_ENDS_WITH_QUERY.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "Index cannot execute wildcard query efficiently",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.SuboptimalIndexForWildcardQuery",
                "If the performance of this statement using `ENDS WITH` doesn't meet your expectations check out the alternative index-providers, see "
                        + "documentation on index configuration.",
                NotificationCategory.PERFORMANCE);
    }

    @Test
    void shouldConstructNotificationsFor_CODE_GENERATION_FAILED() {
        Notification notification = CODE_GENERATION_FAILED.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.CodeGenerationFailed",
                "The database was unable to generate code for the query. A stacktrace can be found in the debug.log.",
                NotificationCategory.PERFORMANCE);
    }

    @Test
    void shouldConstructNotificationsFor_SUBQUERY_VARIABLE_SHADOWING() {
        Notification notification = SUBQUERY_VARIABLE_SHADOWING.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "Variable in subquery is shadowing a variable with the same name from the outer scope.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Statement.SubqueryVariableShadowing",
                "Variable in subquery is shadowing a variable with the same name from the outer scope. "
                        + "If you want to use that variable instead, it must be imported into the subquery using importing WITH clause.",
                NotificationCategory.GENERIC);
    }

    @Test
    void shouldConstructNotificationsFor_HOME_DATABASE_NOT_PRESENT() {
        Notification notification = HOME_DATABASE_NOT_PRESENT.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "The request referred to a home database that does not exist.",
                SeverityLevel.INFORMATION,
                "Neo.ClientNotification.Database.HomeDatabaseNotFound",
                "The home database provided does not currently exist in the DBMS. This command will not take effect until this database is created.",
                NotificationCategory.UNRECOGNIZED);
    }

    @Test
    void shouldConstructNotificationsFor_DEPRECATED_DATABASE_NAME() {
        Notification notification = DEPRECATED_DATABASE_NAME.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "Databases and aliases with unescaped `.` are deprecated unless to indicate that they belong to a composite database. "
                        + "Names containing `.` should be escaped.",
                NotificationCategory.DEPRECATION);
    }

    @Test
    void shouldConstructNotificationsFor_UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION() {
        Notification notification = UNSATISFIABLE_RELATIONSHIP_TYPE_EXPRESSION.notification(
                InputPosition.empty, unsatisfiableRelTypeExpression("!%"));

        verifyNotification(
                notification,
                "The query contains a relationship type expression that cannot be satisfied.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.UnsatisfiableRelationshipTypeExpression",
                "Relationship type expression cannot possibly be satisfied. (`!%` can never be satisfied by any relationship. Relationships must have exactly one relationship type.)",
                NotificationCategory.GENERIC);
    }

    @Test
    void shouldConstructNotificationsFor_REPEATED_RELATIONSHIP_REFERENCE() {
        Notification notification =
                REPEATED_RELATIONSHIP_REFERENCE.notification(InputPosition.empty, repeatedRelationship("r"));

        verifyNotification(
                notification,
                "The query returns no results due to repeated references to a relationship.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RepeatedRelationshipReference",
                "A relationship is referenced more than once in the query, which leads to no results because relationships must not occur more than once in each result. (Relationship `r` was repeated)",
                NotificationCategory.GENERIC);
    }

    @Test
    void shouldConstructNotificationsFor_REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE() {
        Notification notification =
                REPEATED_VAR_LENGTH_RELATIONSHIP_REFERENCE.notification(InputPosition.empty, repeatedRelationship("r"));

        verifyNotification(
                notification,
                "The query returns no results due to repeated references to a relationship.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.RepeatedRelationshipReference",
                "A variable-length relationship variable is bound more than once, which leads to no results because relationships must not occur more than once in each result. (Relationship `r` was repeated)",
                NotificationCategory.GENERIC);
    }

    @Test
    void shouldConstructNotificationsFor_UNION_RETURN_ORDER() {
        Notification notification = UNION_RETURN_ORDER.notification(InputPosition.empty);

        verifyNotification(
                notification,
                "This feature is deprecated and will be removed in future versions.",
                SeverityLevel.WARNING,
                "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
                "All subqueries in a UNION [ALL] should have the same ordering for the return columns. Using differently ordered return items in a UNION [ALL] clause is deprecated and will be removed in a future version.",
                NotificationCategory.DEPRECATION);
    }

    private void verifyNotification(
            Notification notification,
            String title,
            SeverityLevel severity,
            String code,
            String description,
            NotificationCategory category) {
        assertThat(notification.getTitle()).isEqualTo(title);
        assertThat(notification.getSeverity()).isEqualTo(severity);
        assertThat(notification.getCode()).isEqualTo(code);
        assertThat(notification.getDescription()).isEqualTo(description);
        assertThat(notification.getCategory()).isEqualTo(category);
    }

    @Test
    void allNotificationsShouldBeAClientNotification() {

        Arrays.stream(NotificationCodeWithDescription.values()).forEach(notification -> assertThat(
                        notification.notification(InputPosition.empty).getCode())
                .contains("ClientNotification"));
    }

    @Test
    void noNotificationShouldHaveUnknownCategory() {
        Arrays.stream(NotificationCodeWithDescription.values()).forEach(notification -> assertThat(
                        notification.notification(InputPosition.empty).getCategory())
                .isNotEqualTo(NotificationCategory.UNKNOWN));
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
        Arrays.stream(NotificationCodeWithDescription.values()).forEach(notificationCode -> {
            Notification notification = notificationCode.notification(InputPosition.empty);

            // Covers all notification information except NotificationDetail and position, which are query dependent
            notificationBuilder.append(notification.getTitle());
            notificationBuilder.append(notification.getDescription());
            notificationBuilder.append(notification.getCode());
            notificationBuilder.append(notification.getSeverity());
            notificationBuilder.append(notification.getCategory());
        });

        byte[] notificationHash = DigestUtils.sha256(notificationBuilder.toString());

        byte[] expectedHash = new byte[] {
            -86, -2, -31, 42, 124, 55, 109, -39, -73, -67, 61, 104, 114, -30, -2, -128, -115, 110, 73, -35, -2, -86,
            -114, -9, -68, 30, -84, -115, -68, -12, -66, 67
        };

        if (!Arrays.equals(notificationHash, expectedHash)) {
            fail("Expected: " + Arrays.toString(expectedHash) + " \n Actual: " + Arrays.toString(notificationHash)
                    + "\n If you have added, changed or removed a notification, "
                    + "please follow the process on https://trello.com/c/9L3lbeSY/27-update-to-notification-name");
        }
    }
}
