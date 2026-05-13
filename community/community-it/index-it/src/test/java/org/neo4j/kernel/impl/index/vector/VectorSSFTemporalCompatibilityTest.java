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
package org.neo4j.kernel.impl.index.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.index.vector.VectorSSFQueryResult.field;
import static org.neo4j.kernel.impl.index.vector.VectorSSFTemporalTestHelper.generateTestZonedDateTimeStrings;
import static org.neo4j.kernel.impl.index.vector.VectorSSFTemporalTestHelper.generateTestZonedTimeStrings;
import static org.neo4j.kernel.impl.index.vector.VectorSSFTemporalTestHelper.sortByDateTimeZoneOffsetAndId;
import static org.neo4j.kernel.impl.index.vector.VectorSSFTemporalTestHelper.sortByTimeZoneOffsetAndId;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.assertj.core.api.AbstractBooleanAssert;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator;
import org.neo4j.kernel.impl.index.vector.VectorSSFQueryResult.ResultList;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@RandomSupportExtension
public class VectorSSFTemporalCompatibilityTest extends VectorSSFTestBase {

    @Inject
    protected RandomSupport random;

    @Test
    public void checkCypherDateTimesOrder() {
        List<String> allDateTimes = sortByDateTimeZoneOffsetAndId(generateTestZonedDateTimeStrings());

        long previousEpoch = Long.MIN_VALUE;
        String previousDate = null;
        DateTimeValue previousDateTime = DateTimeValue.MIN_VALUE;
        for (String date : allDateTimes) {
            DateTimeValue dateTime = DateTimeValue.parse(date, ZoneId::systemDefault);
            long epoch = dateTime.asObjectCopy().toEpochSecond();
            assertThat(previousEpoch)
                    .as(
                            "%s with epoch %d is not >= previous %d with epoch %d\n",
                            date, epoch, previousDate, previousEpoch)
                    .isLessThanOrEqualTo(epoch);
            assertThat(Values.COMPARATOR.compare(previousDateTime, dateTime))
                    .as("Expected %s(%s) strictly < %s(%s)", previousDate, previousDateTime, date, dateTime)
                    .isNegative();
            if (previousDate != null) {
                assertThatCypherDateTimes(previousDate, ComparisonOperator.LESS_THAN, date)
                        .isTrue();
            }
            previousEpoch = epoch;
            previousDate = date;
            previousDateTime = dateTime;
        }
    }

    @Test
    public void checkCypherTimesOrder() {
        List<String> allDateTimes = sortByTimeZoneOffsetAndId(generateTestZonedTimeStrings());

        String previousDate = null;
        TimeValue previousTime = TimeValue.MIN_VALUE;
        for (String date : allDateTimes) {
            TimeValue time = TimeValue.parse(date, ZoneId::systemDefault);
            assertThat(Values.COMPARATOR.compare(previousTime, time))
                    .as("Expected %s(%s) strictly < %s(%s)", previousDate, previousTime, date, time)
                    .isNegative();
            if (previousDate != null) {
                assertThatCypherTimes(previousDate, ComparisonOperator.LESS_THAN, date)
                        .isTrue();
            }
            previousDate = date;
            previousTime = time;
        }
    }

    @Test
    public void checkSSFExactParameterDateTimesOrderRespectsCypher() throws Exception {
        List<String> allDateTimes = generateTestZonedDateTimeStrings();
        assertThatSSFExactParameterOrderRespectsCypher(
                        "birthdate", allDateTimes, datetime -> DateTimeValue.parse(datetime, ZoneId::systemDefault))
                .isTrue();
    }

    @Test
    public void checkSSFExactParameterTimesOrderRespectsCypher() throws Exception {

        List<String> allTimes = generateTestZonedTimeStrings();
        assertThatSSFExactParameterOrderRespectsCypher(
                        "alarmtime", allTimes, time -> TimeValue.parse(time, ZoneId::systemDefault))
                .isTrue();
    }

    @Test
    public void checkSSFRangeParameterDateTimesOrderRespectsCypher() throws Exception {

        List<String> allDateTimes = sortByDateTimeZoneOffsetAndId(generateTestZonedDateTimeStrings());
        assertThatSSFRangeParameterOrderRespectsCypher(
                        "birthdatetime", allDateTimes, dateTime -> DateTimeValue.parse(dateTime, ZoneId::systemDefault))
                .isTrue();
    }

    /// random seed used: 1764585918538L
    /// checkSSFRangeParameterDateTimesOrderRespectsCypher
    @Test
    public void checkTemporalWithZoneFix() throws Exception {
        List<String> allDateTimes = List.of(
                "2019-06-02T21:00:00.000[Atlantic/Cape_Verde]",
                "2019-06-02T21:00:00.000[Etc/GMT+1]",
                "2019-06-02T22:00:00.000+0000",
                "2019-06-02T22:00:00.000[Africa/Abidjan]",
                "2019-06-02T22:00:00.000[Africa/Accra]",
                "2019-06-02T22:00:00.000[Africa/Bamako]");

        List<String> names = randomNames(allDateTimes.size());
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "name", "birthdatetime");
        // Don't USE embedding 0, as we always query for that...
        int embeddings = EMBEDDINGS.count() - 1;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < allDateTimes.size(); i++) {
                DateTimeValue datetime = DateTimeValue.parse(allDateTimes.get(i), ZoneId::systemDefault);
                createTestNode(
                        tx,
                        Map.of(
                                "id",
                                i,
                                "name",
                                names.get(i),
                                "birthdatetime",
                                datetime,
                                EMBEDDING_NAME,
                                EMBEDDINGS.get((i % embeddings) + 1)));
            }
            tx.commit();
        }
        DateTimeValue fromTime = DateTimeValue.parse(allDateTimes.get(2), ZoneId::systemDefault);
        DateTimeValue toTime = DateTimeValue.parse(allDateTimes.get(4), ZoneId::systemDefault);
        DateTimeValue midTime = DateTimeValue.parse(allDateTimes.get(3), ZoneId::systemDefault);
        assertThat(Values.COMPARATOR.compare(fromTime, toTime)).isNegative();
        assertThat(Values.COMPARATOR.compare(fromTime, midTime)).isNegative();
        assertThat(Values.COMPARATOR.compare(midTime, toTime)).isNegative();
        assertThatCypherDateTimes(allDateTimes.get(2), ComparisonOperator.LESS_THAN, allDateTimes.get(4))
                .isTrue();
        assertThatCypherDateTimes(allDateTimes.get(2), ComparisonOperator.LESS_THAN, allDateTimes.get(3))
                .isTrue();
        assertThatCypherDateTimes(allDateTimes.get(3), ComparisonOperator.LESS_THAN, allDateTimes.get(4))
                .isTrue();
        assertThatCypherDateTimes(allDateTimes.get(2), ComparisonOperator.GREATER_THAN, allDateTimes.get(4))
                .isFalse();
        assertThatCypherDateTimes(allDateTimes.get(2), ComparisonOperator.GREATER_THAN, allDateTimes.get(3))
                .isFalse();
        assertThatCypherDateTimes(allDateTimes.get(3), ComparisonOperator.GREATER_THAN, allDateTimes.get(4))
                .isFalse();

        int RESULT_SIZE = 50;
        ResultList results = queryNodeIndex(
                RESULT_SIZE, allQuery("name"), rangeQuery("birthdatetime", fromTime, true, toTime, true));
        results.sort(Comparator.comparing(v -> ((Integer) v.getValue("id").asObjectCopy())));
        assertThat(results).hasSize(3);
    }

    @Test
    public void checkSSFOpenLowerRangeParameterDateTimesOrderRespectsCypher() throws Exception {

        List<String> allDateTimes = sortByDateTimeZoneOffsetAndId(generateTestZonedDateTimeStrings());
        assertThatSSFOpenLowerRangeParameterOrderRespectsCypher(
                        "birthdatetime", allDateTimes, datetime -> DateTimeValue.parse(datetime, ZoneId::systemDefault))
                .isTrue();
    }

    @Test
    public void checkSSFRangeParameterTimesOrderRespectsCypher() throws Exception {

        List<String> allTimes = sortByTimeZoneOffsetAndId(generateTestZonedTimeStrings());
        assertThatSSFRangeParameterOrderRespectsCypher(
                        "alarmtime", allTimes, time -> TimeValue.parse(time, ZoneId::systemDefault))
                .isTrue();
    }

    @Test
    public void checkSSFOpenLowerRangeParameterTimesOrderRespectsCypher() throws Exception {
        List<String> allTimes = sortByTimeZoneOffsetAndId(generateTestZonedTimeStrings());
        assertThatSSFOpenLowerRangeParameterOrderRespectsCypher(
                        "alarmtime", allTimes, time -> TimeValue.parse(time, ZoneId::systemDefault))
                .isTrue();
    }

    @Test
    public void checkSSFOpenUpperRangeParameterTimesOrderRespectsCypher() throws Exception {

        List<String> allTimes = sortByTimeZoneOffsetAndId(generateTestZonedTimeStrings());
        assertThatSSFOpenUpperRangeParameterOrderRespectsCypher(
                        "alarmtime", allTimes, time -> TimeValue.parse(time, ZoneId::systemDefault))
                .isTrue();
    }

    @Test
    public void checkSSFOpenUpperRangeParameterDateTimesOrderRespectsCypher() throws Exception {

        List<String> allDateTimes = sortByDateTimeZoneOffsetAndId(generateTestZonedDateTimeStrings());
        assertThatSSFOpenUpperRangeParameterOrderRespectsCypher(
                        "birthdatetime", allDateTimes, datetime -> DateTimeValue.parse(datetime, ZoneId::systemDefault))
                .isTrue();
    }

    private AbstractBooleanAssert<?> assertThatCypherDateTimes(String lhs, ComparisonOperator op, String rhs) {
        String query = "RETURN " + op.toPredicateString("datetime($lhs)", "datetime($rhs)") + " AS comp;";
        return assertThat((Boolean) db.executeTransactionally(
                query, Map.of("lhs", lhs, "rhs", rhs), r -> r.columnAs("comp").next()));
    }

    private AbstractBooleanAssert<?> assertThatCypherTimes(String lhs, ComparisonOperator op, String rhs) {
        String query = "RETURN " + op.toPredicateString("time($lhs)", "time($rhs)") + " AS comp;";
        return assertThat((Boolean) db.executeTransactionally(
                query, Map.of("lhs", lhs, "rhs", rhs), r -> r.columnAs("comp").next()));
    }

    private <T extends Value> AbstractBooleanAssert<?> assertThatSSFExactParameterOrderRespectsCypher(
            String fieldName, List<String> inputs, Function<String, T> mapper) throws Exception {
        List<String> names = randomNames(inputs.size());
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "name", fieldName);
        createNodesForExactAndRangeParameterChecks(fieldName, inputs, mapper, names);

        for (int i = 0; i < inputs.size(); i++) {
            T time = mapper.apply(inputs.get(i));
            assertThat(queryNodeIndex(allQuery("name"), exactQuery(fieldName, time)))
                    .as("%d name %s %s %s", i, names.get(i), fieldName, time)
                    .hasSize(1)
                    .have(field("name", names.get(i)));
        }
        return assertThat(true);
    }

    private <T extends Value> void createNodesForExactAndRangeParameterChecks(
            String fieldName, List<String> inputs, Function<String, T> mapper, List<String> names) {

        int embeddings = EMBEDDINGS.count() - 1;
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < inputs.size(); i++) {
                T timeValue = mapper.apply(inputs.get(i));
                createTestNode(
                        tx,
                        Map.of(
                                "id",
                                i,
                                "name",
                                names.get(i),
                                fieldName,
                                timeValue,
                                EMBEDDING_NAME,
                                EMBEDDINGS.get((i % embeddings) + 1)));
            }
            tx.commit();
        }
    }

    private <T extends Value> AbstractBooleanAssert<?> assertThatSSFRangeParameterOrderRespectsCypher(
            String fieldName, List<String> inputs, Function<String, T> mapper) throws Exception {

        List<String> names = randomNames(inputs.size());
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "name", fieldName);
        createNodesForExactAndRangeParameterChecks(fieldName, inputs, mapper, names);

        int repeat = 100;
        int resultSize = 50;
        for (int i = 0; i < repeat; i++) {
            int lower = random.nextInt(inputs.size());
            int delta = random.nextInt(Math.min(inputs.size() - lower, resultSize - 1));
            int upper = lower + delta;
            T fromTime = mapper.apply(inputs.get(lower));
            T toTime = mapper.apply(inputs.get(upper));
            ResultList results =
                    queryNodeIndex(resultSize, allQuery("name"), rangeQuery(fieldName, fromTime, true, toTime, true));
            results.sort(Comparator.comparing(v -> ((Integer) v.getValue("id").asObjectCopy())));
            assertThat(results)
                    .as("%d: From %d(%s) to %d(%s)", i, lower, fromTime, upper, toTime)
                    .hasSize(upper - lower + 1);
            int j = lower;
            for (VectorSSFQueryResult result : results) {
                assertThat(result).has(field("name", names.get(j)));
                j += 1;
            }
        }
        return assertThat(true);
    }

    private <T extends Value> List<Integer> createNodesForOpenRangeCheck(
            String fieldName, List<String> inputs, Function<String, T> mapper, List<String> names, int numberToCreate) {

        int embeddings = EMBEDDINGS.count() - 1;
        int creationCount = 0;
        List<Integer> timesWithNode = new ArrayList<>();
        double creationProbability = ((double) numberToCreate) / inputs.size();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < inputs.size(); i++) {
                if (random.nextDouble() < creationProbability && creationCount < numberToCreate) {
                    T datetime = mapper.apply(inputs.get(i));
                    createTestNode(
                            tx,
                            Map.of(
                                    "id",
                                    i,
                                    "name",
                                    names.get(i),
                                    fieldName,
                                    datetime,
                                    EMBEDDING_NAME,
                                    EMBEDDINGS.get((i % embeddings) + 1)));
                    timesWithNode.add(i);
                    creationCount++;
                }
            }
            tx.commit();
        }
        return timesWithNode;
    }

    private <T extends Value> AbstractBooleanAssert<?> assertThatSSFOpenLowerRangeParameterOrderRespectsCypher(
            String fieldName, List<String> inputs, Function<String, T> mapper) throws Exception {

        List<String> names = randomNames(inputs.size());
        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "name", fieldName);
        int RESULT_SIZE = 50;
        List<Integer> timesWithNode = createNodesForOpenRangeCheck(fieldName, inputs, mapper, names, RESULT_SIZE);
        // Don't USE embedding 0, as we always query for that...

        int repeat = 100;
        for (int i = 0; i < repeat; i++) {
            int upper = random.nextInt(inputs.size());
            T toTime = mapper.apply(inputs.get(upper));
            ResultList results = queryNodeIndex(
                    RESULT_SIZE, allQuery("name"), rangeQuery(fieldName, Values.NO_VALUE, true, toTime, true));
            results.sort(Comparator.comparing(v -> ((Integer) v.getValue("id").asObjectCopy())));
            int expected = 0;
            for (Integer nodeIndex : timesWithNode) {
                if (nodeIndex > upper) break;
                expected += 1;
            }
            assertThat(results)
                    .as("%d: From NO_VALUE to %d(%s)", i, upper, toTime)
                    .hasSize(expected);
            int j = 0;
            for (VectorSSFQueryResult result : results) {
                String name = names.get(timesWithNode.get(j));
                assertThat(result).has(field("name", name));
                j += 1;
            }
        }
        return assertThat(true);
    }

    private List<String> randomNames(int count) {
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add(random.nextAlphaNumericString(4, 12));
        }
        return names;
    }

    private <T extends Value> AbstractBooleanAssert<?> assertThatSSFOpenUpperRangeParameterOrderRespectsCypher(
            String fieldName, List<String> inputs, Function<String, T> mapper) throws Exception {

        List<String> names = randomNames(inputs.size());

        createNodeVectorIndex(VECTOR_INDEX_NAME, EMBEDDINGS.dimensions(), EMBEDDING_NAME, "name", fieldName);
        int RESULT_SIZE = 50;
        List<Integer> timesWithNode = createNodesForOpenRangeCheck(fieldName, inputs, mapper, names, RESULT_SIZE);

        int repeat = 100;
        for (int i = 0; i < repeat; i++) {
            int lower = random.nextInt(inputs.size());
            T fromTime = mapper.apply(inputs.get(lower));
            ResultList results = queryNodeIndex(
                    RESULT_SIZE, allQuery("name"), rangeQuery(fieldName, fromTime, true, Values.NO_VALUE, true));
            results.sort(Comparator.comparing(v -> ((Integer) v.getValue("id").asObjectCopy())));
            int firstExpectedIndex = 0;
            for (Integer nodeIndex : timesWithNode) {
                if (nodeIndex >= lower) break;
                firstExpectedIndex += 1;
            }
            int expected = timesWithNode.size() - firstExpectedIndex;
            assertThat(results)
                    .as("%d: From %d(%s) to NO_VALUE", i, lower, fromTime)
                    .hasSize(expected);
            int j = firstExpectedIndex;
            for (VectorSSFQueryResult result : results) {
                String name = names.get(timesWithNode.get(j));
                assertThat(result).has(field("name", name));
                j += 1;
            }
        }
        return assertThat(true);
    }
}
