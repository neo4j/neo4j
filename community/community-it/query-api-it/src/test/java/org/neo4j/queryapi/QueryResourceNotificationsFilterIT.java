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
package org.neo4j.queryapi;

import static org.neo4j.queryapi.QueryResponseAssertions.assertThat;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;

@QueryAPITestExtension
public class QueryResourceNotificationsFilterIT {

    private final String queryEndpoint;

    QueryResourceNotificationsFilterIT(QueryAPITestClient testClient) {
        this.queryEndpoint = testClient.getEndpoint();
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldFilterNotificationsUsingMinimumSeverityLevelWARNING(
            TransactionType transactionType, QueryContentType contentType) throws Exception {
        var response = transactionType.begin(
                new QueryAPITestClient(this.queryEndpoint, contentType),
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(
                                QueryRequest.NotificationsFilterBuilder.newBuilderWithMinimumSeverityLevel("WARNING")
                                        .build())
                        .build());

        QueryResponseAssertions.assertThat(response)
                .wasSuccessful()
                .hasNotifications(
                        NotificationCodeWithDescription.MISSING_LABEL, NotificationCodeWithDescription.MISSING_LABEL);
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldFilterNotificationsUsingMinimumSeverityLevelINFORMATION(
            TransactionType transactionType, QueryContentType contentType) throws Exception {
        var response = transactionType.begin(
                new QueryAPITestClient(this.queryEndpoint, contentType),
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(QueryRequest.NotificationsFilterBuilder.newBuilderWithMinimumSeverityLevel(
                                        "INFORMATION")
                                .build())
                        .build());

        QueryResponseAssertions.assertThat(response)
                .wasSuccessful()
                .hasNotifications(
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.CARTESIAN_PRODUCT);
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldFilterNotificationsUsingMinimumSeverityLevelOFF(
            TransactionType transactionType, QueryContentType contentType) throws Exception {
        var response = transactionType.begin(
                new QueryAPITestClient(this.queryEndpoint, contentType),
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(
                                QueryRequest.NotificationsFilterBuilder.newBuilderWithMinimumSeverityLevel("OFF")
                                        .build())
                        .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasNoNotifications();
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldFilterNotificationsFilterRejectUnknowSeverity(
            TransactionType transactionType, QueryContentType contentType) throws Exception {
        var response = transactionType.begin(
                new QueryAPITestClient(this.queryEndpoint, contentType),
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(
                                QueryRequest.NotificationsFilterBuilder.newBuilderWithMinimumSeverityLevel("DUNO")
                                        .build())
                        .build());

        QueryResponseAssertions.assertThat(response).hasErrorStatus(400, Status.Request.Invalid);
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldFilterNotificationsUsingDisabledCategoriesUNRECOGNIZED(
            TransactionType transactionType, QueryContentType contentType) throws Exception {
        var response = transactionType.begin(
                new QueryAPITestClient(this.queryEndpoint, contentType),
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(
                                QueryRequest.NotificationsFilterBuilder.newBuilderWithDisabledCategories("UNRECOGNIZED")
                                        .build())
                        .build());

        QueryResponseAssertions.assertThat(response)
                .wasSuccessful()
                .hasNotifications(NotificationCodeWithDescription.CARTESIAN_PRODUCT);
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldFilterNotificationsUsingDisabledCategoriesPERFORMANCE(
            TransactionType transactionType, QueryContentType contentType) throws Exception {
        var response = transactionType.begin(
                new QueryAPITestClient(this.queryEndpoint, contentType),
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(
                                QueryRequest.NotificationsFilterBuilder.newBuilderWithDisabledCategories("PERFORMANCE")
                                        .build())
                        .build());

        QueryResponseAssertions.assertThat(response)
                .wasSuccessful()
                .hasNotifications(
                        NotificationCodeWithDescription.MISSING_LABEL, NotificationCodeWithDescription.MISSING_LABEL);
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldFilterNotificationsUsingDisabledCategoriesPERFORMANCEandUNRECOGNIZED(
            TransactionType transactionType, QueryContentType contentType) throws Exception {
        var response = transactionType.begin(
                new QueryAPITestClient(this.queryEndpoint, contentType),
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(QueryRequest.NotificationsFilterBuilder.newBuilderWithDisabledCategories(
                                        "PERFORMANCE", "UNRECOGNIZED")
                                .build())
                        .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasNoNotifications();
    }

    @ParameterizedTest
    @MethodSource("shouldFilterNotificationsUsingAnListOfKnownCategoriesFixtures")
    void shouldFilterNotificationsUsingAnListOfKnownCategories(
            TransactionType transactionType, QueryContentType contentType, String[] disabledCategories)
            throws Exception {
        var response = transactionType.begin(
                new QueryAPITestClient(this.queryEndpoint, contentType),
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(QueryRequest.NotificationsFilterBuilder.newBuilderWithDisabledCategories(
                                        disabledCategories)
                                .build())
                        .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldFilterNotificationsRejectUnknownCategories(TransactionType transactionType, QueryContentType contentType)
            throws Exception {
        var response = transactionType.begin(
                new QueryAPITestClient(this.queryEndpoint, contentType),
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(
                                QueryRequest.NotificationsFilterBuilder.newBuilderWithDisabledCategories("DUNO")
                                        .build())
                        .build());

        QueryResponseAssertions.assertThat(response).hasErrorStatus(400, Status.Request.Invalid);
    }

    @ParameterizedTest
    @MethodSource("contentTypes")
    void shouldRejectFilterNotificationsUsingMinimumSeverityLevelOnMiddleTransaction(QueryContentType contentType)
            throws Exception {

        var typedClient = new QueryAPITestClient(queryEndpoint, contentType);

        var res = typedClient.beginTx();
        assertThat(res).wasSuccessful();

        var continueTx = typedClient.runInTx(
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(
                                QueryRequest.NotificationsFilterBuilder.newBuilderWithMinimumSeverityLevel("WARNING")
                                        .build())
                        .build(),
                res.body().txId());

        assertThat(continueTx).hasErrorStatus(400, Status.Request.Invalid);
    }

    @ParameterizedTest
    @MethodSource("contentTypes")
    void shouldRejectFilterNotificationsUsingDisabledCategoriesOnMiddleTransaction(QueryContentType contentType)
            throws Exception {

        var typedClient = new QueryAPITestClient(queryEndpoint, contentType);

        var res = typedClient.beginTx();
        assertThat(res).wasSuccessful();

        var continueTx = typedClient.runInTx(
                QueryRequest.newBuilder()
                        .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                        .notificationsFilter(
                                QueryRequest.NotificationsFilterBuilder.newBuilderWithDisabledCategories("UNRECOGNIZED")
                                        .build())
                        .build(),
                res.body().txId());

        assertThat(continueTx).hasErrorStatus(400, Status.Request.Invalid);
    }

    public static Stream<Arguments> transactionTypes() {
        return Stream.of(TransactionType.values())
                .flatMap(transactionType -> QueryContentType.inputContentTypes()
                        .map(contentType -> Arguments.of(transactionType, contentType)));
    }

    public static Stream<Arguments> shouldFilterNotificationsUsingAnListOfKnownCategoriesFixtures() {
        return Stream.of(TransactionType.values())
                .flatMap(transactionType -> QueryContentType.inputContentTypes()
                        .flatMap(contentType -> Stream.of(
                                        List.of(),
                                        List.of("HINT"),
                                        List.of("HINT", "UNRECOGNIZED"),
                                        List.of("HINT", "UNRECOGNIZED", "UNSUPPORTED"),
                                        List.of("HINT", "UNRECOGNIZED", "UNSUPPORTED", "PERFORMANCE"),
                                        List.of("HINT", "UNRECOGNIZED", "UNSUPPORTED", "PERFORMANCE", "TOPOLOGY"),
                                        List.of(
                                                "HINT",
                                                "UNRECOGNIZED",
                                                "UNSUPPORTED",
                                                "PERFORMANCE",
                                                "TOPOLOGY",
                                                "SECURITY"),
                                        List.of(
                                                "HINT",
                                                "UNRECOGNIZED",
                                                "UNSUPPORTED",
                                                "PERFORMANCE",
                                                "TOPOLOGY",
                                                "SECURITY",
                                                "DEPRECATION"),
                                        List.of(
                                                "HINT",
                                                "UNRECOGNIZED",
                                                "UNSUPPORTED",
                                                "PERFORMANCE",
                                                "TOPOLOGY",
                                                "SECURITY",
                                                "DEPRECATION",
                                                "GENERIC"),
                                        List.of(
                                                "HINT",
                                                "UNRECOGNIZED",
                                                "UNSUPPORTED",
                                                "PERFORMANCE",
                                                "TOPOLOGY",
                                                "SECURITY",
                                                "DEPRECATION",
                                                "GENERIC",
                                                "SCHEMA"))
                                .map(list -> list.toArray(new String[] {}))
                                .map(disabledCategories ->
                                        Arguments.of(transactionType, contentType, disabledCategories))));
    }

    public static Stream<Arguments> contentTypes() {
        return QueryContentType.inputContentTypes().map(Arguments::of);
    }
}
