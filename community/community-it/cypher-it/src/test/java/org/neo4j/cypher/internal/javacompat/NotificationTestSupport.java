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
package org.neo4j.cypher.internal.javacompat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.conditions.Conditions.instanceOf;

import java.util.stream.Stream;
import org.assertj.core.api.Condition;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.NotificationCategory;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.notifications.NotificationImplementation;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;
import org.neo4j.test.conditions.Conditions;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
public class NotificationTestSupport {
    @Inject
    protected GraphDatabaseAPI db;

    void assertNotifications(String query, Condition<Iterable<? extends Notification>> matchesExpectation) {
        try (Transaction transaction = db.beginTx();
                Result result = transaction.execute(query)) {
            assertThat(result.getNotifications()).is(matchesExpectation);
        }
    }

    public static Condition<Notification> notification(
            String code,
            Condition<String> descriptionCondition,
            Condition<InputPosition> positionCondition,
            SeverityLevel severity,
            NotificationCategory category) {
        final var description = "Notification{code=%s, description=[%s], position=[%s], severity=%s}"
                .formatted(code, descriptionCondition.description(), positionCondition.description(), severity);

        return new Condition<>(
                notification -> code.equals(notification.getCode())
                        && descriptionCondition.matches(notification.getDescription())
                        && positionCondition.matches(notification.getPosition())
                        && severity.equals(notification.getSeverity())
                        && category.equals(notification.getCategory()),
                description);
    }

    public static Condition<Iterable<? extends Notification>> contains(Condition<Notification> condition) {
        return new Condition<>(
                notifications -> {
                    for (var notification : notifications) {
                        if (condition.matches(notification)) {
                            return true;
                        }
                    }
                    return false;
                },
                "an iterable containing " + condition.description());
    }

    public static Condition<Iterable<? extends Notification>> doesNotContain(Condition<Notification> condition) {
        return new Condition<>(
                notifications -> {
                    for (var notification : notifications) {
                        if (condition.matches(notification)) {
                            return false;
                        }
                    }
                    return true;
                },
                "an iterable not containing " + condition.description());
    }

    void shouldNotifyInStream(String query, NotificationImplementation expectedNotification) {
        try (Transaction transaction = db.beginTx()) {
            // when
            try (Result result = transaction.execute(query)) {
                // then
                assertThat(result.getNotifications()).contains(expectedNotification);
            }
            transaction.commit();
        }
    }

    void shouldNotNotifyInStream(String query) {
        try (Transaction transaction = db.beginTx()) {
            // when
            try (Result result = transaction.execute(query)) {
                // then
                assertThat(result.getNotifications()).isEmpty();
            }
            transaction.commit();
        }
    }

    Condition<Notification> cartesianProductNotification = notification(
            "Neo.ClientNotification.Statement.CartesianProduct",
            Conditions.contains(
                    "If a part of a query contains multiple disconnected patterns, this will build a "
                            + "cartesian product between all those parts. This may produce a large amount of data and slow down"
                            + " query processing. "
                            + "While occasionally intended, it may often be possible to reformulate the query that avoids the "
                            + "use of this cross "
                            + "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH"),
            instanceOf(InputPosition.class),
            SeverityLevel.INFORMATION,
            NotificationCategory.PERFORMANCE);

    Condition<Notification> largeLabelCSVNotification = notification(
            "Neo.ClientNotification.Statement.NoApplicableIndex",
            Conditions.contains("Using LOAD CSV with a large data set in a query where the execution plan contains the "
                    + "Using LOAD CSV followed by a MATCH or MERGE that matches a non-indexed label will most likely "
                    + "not perform well on large data sets. Please consider using a schema index."),
            instanceOf(InputPosition.class),
            SeverityLevel.INFORMATION,
            NotificationCategory.PERFORMANCE);

    Condition<Notification> eagerOperatorNotification = notification(
            "Neo.ClientNotification.Statement.EagerOperator",
            Conditions.contains("Using LOAD CSV with a large data set in a query where the execution plan contains the "
                    + "Eager operator could potentially consume a lot of memory and is likely to not perform well. "
                    + "See the Neo4j Manual entry on the Eager operator for more information and hints on "
                    + "how problems could be avoided."),
            instanceOf(InputPosition.class),
            SeverityLevel.WARNING,
            NotificationCategory.PERFORMANCE);
    Condition<Notification> unknownPropertyKeyNotification = notification(
            "Neo.ClientNotification.Statement.UnknownPropertyKeyWarning",
            Conditions.contains("the missing property name is"),
            instanceOf(InputPosition.class),
            SeverityLevel.WARNING,
            NotificationCategory.UNRECOGNIZED);

    Condition<Notification> unknownRelationshipNotification = notification(
            "Neo.ClientNotification.Statement.UnknownRelationshipTypeWarning",
            Conditions.contains("the missing relationship type is"),
            instanceOf(InputPosition.class),
            SeverityLevel.WARNING,
            NotificationCategory.UNRECOGNIZED);

    Condition<Notification> unknownLabelNotification = notification(
            "Neo.ClientNotification.Statement.UnknownLabelWarning",
            Conditions.contains("the missing label name is"),
            instanceOf(InputPosition.class),
            SeverityLevel.WARNING,
            NotificationCategory.UNRECOGNIZED);

    Condition<Notification> dynamicPropertyNotification = notification(
            "Neo.ClientNotification.Statement.DynamicProperty",
            Conditions.contains("Using a dynamic property makes it impossible to use an index lookup for this query"),
            instanceOf(InputPosition.class),
            SeverityLevel.INFORMATION,
            NotificationCategory.PERFORMANCE);

    Condition<Notification> deprecatedRelationshipTypeSeparator = notification(
            "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
            Conditions.contains(
                    "The semantics of using colon in the separation of alternative relationship types will change in a future version"),
            instanceOf(InputPosition.class),
            SeverityLevel.WARNING,
            NotificationCategory.DEPRECATION);

    Condition<Notification> DeprecatedConnectComponentsPlannerPreParserOption = notification(
            "Neo.ClientNotification.Statement.FeatureDeprecationWarning",
            Conditions.contains(
                    "The Cypher query option `connectComponentsPlanner` is deprecated and will be removed without a replacement. "
                            + "The product's default behavior of using a cost-based IDP search algorithm when combining sub-plans will be kept. "
                            + "For more information, see Cypher Manual -> Cypher planner."),
            instanceOf(InputPosition.class),
            SeverityLevel.WARNING,
            NotificationCategory.DEPRECATION);

    public static class ChangedResults {
        @Deprecated
        public final String oldField = "deprecated";

        public final String newField = "use this";
    }

    public static class TestProcedures {
        @Procedure("newProc")
        public void newProc() {}

        @Deprecated
        @Procedure(name = "oldProc", deprecatedBy = "newProc")
        public void oldProc() {}

        @Deprecated
        @Procedure(name = "oldProcNotReplaced")
        public void oldProcNotReplaced() {}

        @Procedure("changedProc")
        public Stream<ChangedResults> changedProc() {
            return Stream.of(new ChangedResults());
        }

        @Procedure("changedProc2")
        public void changedProc2(@Name("value") @Deprecated Long value) {}
    }

    public static class TestFunctions {
        @UserFunction("org.example.com.newFunc")
        public Long newFunc() {
            return 2L;
        }

        @UserFunction("org.example.com.FuncWithDepInput")
        public Long func(@Name("value") @Deprecated Long value) {
            return value;
        }

        @Deprecated
        @UserFunction(name = "org.example.com.oldFunc", deprecatedBy = "org.example.com.newFunc")
        public Long oldFunc() {
            return 2L;
        }

        @Deprecated
        @UserFunction(name = "org.example.com.oldFuncNotReplaced")
        public Long oldFuncNotReplaced() {
            return 5L;
        }

        @UserAggregationFunction(name = "org.example.com.newAggFunc")
        public TestAggFunction newAggFunc() {
            return new TestAggFunction();
        }

        @Deprecated
        @UserAggregationFunction(name = "org.example.com.oldAggFunc", deprecatedBy = "org.example.com.newAggFunc")
        public TestAggFunction oldAggFunc() {
            return new TestAggFunction();
        }
    }

    public static class TestAggFunction {
        Long latest = 0L;

        @UserAggregationUpdate
        public void update(@Name("value") Long value) {
            latest = value;
        }

        @UserAggregationResult
        public Long result() {
            return latest;
        }
    }
}
