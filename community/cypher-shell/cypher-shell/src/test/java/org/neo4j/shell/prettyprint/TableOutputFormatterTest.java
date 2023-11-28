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
package org.neo4j.shell.prettyprint;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.prettyprint.OutputFormatter.NEWLINE;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.InternalRecord;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.DurationValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.PointValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.state.BoltResult;
import org.neo4j.shell.state.ListBoltResult;
import org.neo4j.shell.test.LocaleDependentTestBase;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
class TableOutputFormatterTest extends LocaleDependentTestBase {
    private final PrettyPrinter verbosePrinter = new PrettyPrinter(new PrettyConfig(Format.VERBOSE, true, 100, false));

    @Test
    void prettyPrintPlanInformation() {
        // given
        ResultSummary resultSummary = mock(ResultSummary.class);

        Map<String, Value> argumentMap = Values.parameters(
                        "Version", "3.1",
                        "Planner", "COST",
                        "Runtime", "INTERPRETED",
                        "GlobalMemory", 10,
                        "DbHits", 2,
                        "Rows", 3,
                        "EstimatedRows", 10,
                        "Time", 15,
                        "Order", "a",
                        "PageCacheHits", 22,
                        "PageCacheMisses", 2,
                        "Memory", 5)
                .asMap(v -> v);

        ProfiledPlan plan = mock(ProfiledPlan.class);
        when(plan.dbHits()).thenReturn(1000L);
        when(plan.records()).thenReturn(20L);
        when(plan.arguments()).thenReturn(argumentMap);
        when(plan.operatorType()).thenReturn("MyOp");
        when(plan.identifiers()).thenReturn(Arrays.asList("a", "b"));

        when(resultSummary.hasPlan()).thenReturn(true);
        when(resultSummary.hasProfile()).thenReturn(true);
        when(resultSummary.plan()).thenReturn(plan);
        when(resultSummary.profile()).thenReturn(plan);
        when(resultSummary.resultAvailableAfter(any())).thenReturn(5L);
        when(resultSummary.resultConsumedAfter(any())).thenReturn(7L);
        when(resultSummary.queryType()).thenReturn(QueryType.READ_ONLY);
        when(plan.arguments()).thenReturn(argumentMap);

        BoltResult result = new ListBoltResult(Collections.emptyList(), resultSummary);

        // when
        String actual = verbosePrinter.format(result);

        // then
        var resourceStream = getClass()
                .getResourceAsStream("/org/neo4j/shell/prettyprint/expected-pretty-print-plan-information.txt");
        var expected = new BufferedReader(new InputStreamReader(requireNonNull(resourceStream)))
                .lines()
                .collect(joining(NEWLINE));
        assertThat(actual).startsWith(expected);
    }

    @Test
    void prettyPrintPoint() {
        // given
        List<String> keys = asList("p1", "p2");

        Value point2d = new PointValue(new InternalPoint2D(4326, 42.78, 56.7));
        Value point3d = new PointValue(new InternalPoint3D(4326, 1.7, 26.79, 34.23));
        Record record = new InternalRecord(keys, new Value[] {point2d, point3d});

        // when
        String actual = verbosePrinter.format(new ListBoltResult(asList(record), mock(ResultSummary.class)));

        // then
        assertThat(actual).contains("| point({srid:4326, x:42.78, y:56.7}) |");
        assertThat(actual).contains("| point({srid:4326, x:1.7, y:26.79, z:34.23}) |");
    }

    @Test
    void prettyPrintDuration() {
        // given
        List<String> keys = asList("d");

        Value duration = new DurationValue(new InternalIsoDuration(1, 2, 3, 4));
        Record record = new InternalRecord(keys, new Value[] {duration});

        // when
        String actual = verbosePrinter.format(new ListBoltResult(asList(record), mock(ResultSummary.class)));

        // then
        assertThat(actual).contains("| P1M2DT3.000000004S |");
    }

    @Test
    void prettyPrintDurationWithNoTrailingZeroes() {
        // given
        List<String> keys = asList("d");

        Value duration = new DurationValue(new InternalIsoDuration(1, 2, 3, 0));
        Record record = new InternalRecord(keys, new Value[] {duration});

        // when
        String actual = verbosePrinter.format(new ListBoltResult(asList(record), mock(ResultSummary.class)));

        // then
        assertThat(actual).contains("| P1M2DT3S |");
    }

    @Test
    void prettyPrintNode() {
        // given
        List<String> labels = asList("label1", "label2");
        Map<String, Value> propertiesAsMap = new HashMap<>();
        propertiesAsMap.put("prop1", Values.value("prop1_value"));
        propertiesAsMap.put("prop2", Values.value("prop2_value"));
        List<String> keys = asList("col1", "col2");

        Value value = new NodeValue(new InternalNode(1, labels, propertiesAsMap));
        Record record = new InternalRecord(keys, new Value[] {value});

        // when
        String actual = verbosePrinter.format(new ListBoltResult(asList(record), mock(ResultSummary.class)));

        // then
        assertThat(actual).contains("| (:label1:label2 {prop2: \"prop2_value\", prop1: \"prop1_value\"}) |");
    }

    @Test
    void prettyPrintRelationships() {
        // given
        List<String> keys = asList("rel");

        Map<String, Value> propertiesAsMap = new HashMap<>();
        propertiesAsMap.put("prop1", Values.value("prop1_value"));
        propertiesAsMap.put("prop2", Values.value("prop2_value"));

        RelationshipValue relationship =
                new RelationshipValue(new InternalRelationship(1, 1, 2, "RELATIONSHIP_TYPE", propertiesAsMap));

        Record record = new InternalRecord(keys, new Value[] {relationship});

        // when
        String actual = verbosePrinter.format(new ListBoltResult(asList(record), mock(ResultSummary.class)));

        // then
        assertThat(actual).contains("| [:RELATIONSHIP_TYPE {prop2: \"prop2_value\", prop1: \"prop1_value\"}] |");
    }

    @Test
    void prettyPrintPath() {
        // given
        List<String> keys = asList("path");

        Node n1 = mock(Node.class);
        when(n1.id()).thenReturn(1L);
        List<String> labels = asList("L1");
        when(n1.labels()).thenReturn(labels);
        when(n1.asMap(any())).thenReturn(Collections.emptyMap());

        Relationship r1 = mock(Relationship.class);
        when(r1.startNodeId()).thenReturn(2L);
        when(r1.type()).thenReturn("R1");
        when(r1.asMap(any())).thenReturn(Collections.emptyMap());

        Node n2 = mock(Node.class);
        when(n2.id()).thenReturn(2L);
        when(n2.labels()).thenReturn(asList("L2"));
        when(n2.asMap(any())).thenReturn(Collections.emptyMap());

        Relationship r2 = mock(Relationship.class);
        when(r2.startNodeId()).thenReturn(2L);
        when(r2.type()).thenReturn("R2");
        when(r2.asMap(any())).thenReturn(Collections.emptyMap());

        Node n3 = mock(Node.class);
        when(n3.id()).thenReturn(3L);
        when(n3.labels()).thenReturn(asList("L3"));
        when(n3.asMap(any())).thenReturn(Collections.emptyMap());

        Path.Segment s1 = mock(Path.Segment.class);
        when(s1.relationship()).thenReturn(r1);
        when(s1.start()).thenReturn(n1);
        when(s1.end()).thenReturn(n2);

        Path.Segment s2 = mock(Path.Segment.class);
        when(s2.relationship()).thenReturn(r2);
        when(s2.start()).thenReturn(n2);
        when(s2.end()).thenReturn(n3);

        List<Path.Segment> segments = asList(s1, s2);
        List<Node> nodes = asList(n1, n2);
        List<Relationship> relationships = asList(r1);
        InternalPath internalPath = new InternalPath(segments, nodes, relationships);
        Value value = new PathValue(internalPath);

        Record record = new InternalRecord(keys, new Value[] {value});

        // when
        String actual = verbosePrinter.format(new ListBoltResult(asList(record), mock(ResultSummary.class)));

        // then
        assertThat(actual).contains("| (:L1)<-[:R1]-(:L2)-[:R2]->(:L3) |");
    }

    @Test
    void printRelationshipsAndNodesWithEscapingForSpecialCharacters() {
        // given
        Record record = mock(Record.class);
        Map<String, Value> propertiesAsMap = new HashMap<>();
        propertiesAsMap.put("prop1", Values.value("prop1, value"));
        propertiesAsMap.put("prop2", Values.value(1));
        Value relVal = new RelationshipValue(new InternalRelationship(1, 1, 2, "RELATIONSHIP,TYPE", propertiesAsMap));

        List<String> labels = asList("label `1", "label2");
        Map<String, Value> nodeProperties = new HashMap<>();
        nodeProperties.put("prop1", Values.value("prop1:value"));
        String doubleQuotes = "\"\"";
        nodeProperties.put("1prop1", Values.value(doubleQuotes));
        nodeProperties.put("ä", Values.value("not-escaped"));

        Value nodeVal = new NodeValue(new InternalNode(1, labels, nodeProperties));

        Map<String, Value> recordMap = new LinkedHashMap<>();
        recordMap.put("rel", relVal);
        recordMap.put("node", nodeVal);
        List<String> keys = asList("rel", "node");
        when(record.keys()).thenReturn(keys);
        when(record.size()).thenReturn(2);
        when(record.get(0)).thenReturn(relVal);
        when(record.get(1)).thenReturn(nodeVal);

        when(record.<Value>asMap(any())).thenReturn(recordMap);

        when(record.values()).thenReturn(asList(relVal, nodeVal));

        // when
        String actual = verbosePrinter.format(new ListBoltResult(asList(record), mock(ResultSummary.class)));

        // then
        assertThat(actual).contains("| [:`RELATIONSHIP,TYPE` {prop2: 1, prop1: \"prop1, value\"}] |");
        assertThat(actual)
                .contains("| (:`label ``1`:label2 {`1prop1`: \"\\\"\\\"\", "
                        + "prop1: \"prop1:value\", ä: \"not-escaped\"}) |");
    }

    @Test
    void basicTable() {
        // GIVEN
        Result result = mockResult(asList("c1", "c2"), "a", 42);
        // WHEN
        String table = formatResult(result);
        // THEN
        assertThat(table).contains("| c1  | c2 |");
        assertThat(table).contains("| \"a\" | 42 |");
    }

    @Test
    void twoRowsWithNumbersAllSampled() {
        // GIVEN
        Result result = mockResult(asList("c1", "c2"), "a", 42, "b", 43);
        // WHEN
        String table = formatResult(result);
        // THEN
        assertThat(table).contains("| \"a\" | 42 |");
        assertThat(table).contains("| \"b\" | 43 |");
    }

    @Test
    void printAllButFooterWhenThrowInTopRecords() {
        // GIVEN
        BoltResult result = mock(BoltResult.class);

        when(result.getKeys()).thenReturn(List.of("i"));
        when(result.iterate()).thenReturn(new ThrowAfterN(5));

        // WHEN
        ToStringLinePrinter actual = new ToStringLinePrinter();

        // THEN
        assertThrows(RuntimeException.class, () -> verbosePrinter.format(result, actual));
        assertThat(actual.result().replaceAll("\\s+", "")).endsWith("|1|".repeat(5));
    }

    @Test
    void printAllButFooterWhenThrowAfterTopRecords() {
        // GIVEN
        BoltResult result = mock(BoltResult.class);

        when(result.getKeys()).thenReturn(List.of("i"));
        when(result.iterate()).thenReturn(new ThrowAfterN(150));

        // WHEN
        ToStringLinePrinter actual = new ToStringLinePrinter();

        // THEN
        assertThrows(RuntimeException.class, () -> verbosePrinter.format(result, actual));
        assertThat(actual.result().replaceAll("\\s+", "")).endsWith("|1|".repeat(150));
    }

    @Test
    void printAllButFooterWhenThrowInFirstRecord() {
        // GIVEN
        BoltResult result = mock(BoltResult.class);

        when(result.getKeys()).thenReturn(List.of("i"));
        when(result.iterate()).thenReturn(new ThrowAfterN(0));

        // WHEN
        ToStringLinePrinter actual = new ToStringLinePrinter();

        // THEN
        assertThrows(RuntimeException.class, () -> verbosePrinter.format(result, actual));
        assertThat(actual.result()).isEqualTo(String.format("+---+%n| i |%n+---+%n"));
    }

    @Test
    void fiveRowsWithNumbersNotAllSampled() {
        // GIVEN
        Result result = mockResult(asList("c1", "c2"), "a", 42, "b", 43, "c", 44, "d", 45, "e", 46);
        // WHEN
        String table = formatResult(result);
        // THEN
        assertThat(table).contains("| \"a\" | 42 |");
        assertThat(table).contains("| \"b\" | 43 |");
        assertThat(table).contains("| \"c\" | 44 |");
        assertThat(table).contains("| \"d\" | 45 |");
        assertThat(table).contains("| \"e\" | 46 |");
    }

    @Test
    void wrapStringContent() {
        // GIVEN
        Result result = mockResult(asList("c1"), "a", "bb", "ccc", "dddd", "eeeee");
        // WHEN
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(true, 2).formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        String table = printer.result();
        // THEN
        assertThat(table)
                .isEqualTo(String.join(
                        NEWLINE,
                        "+------+",
                        "| c1   |",
                        "+------+",
                        "| \"a\"  |",
                        "| \"bb\" |",
                        "| \"ccc |",
                        "\\ \"    |",
                        "| \"ddd |",
                        "\\ d\"   |",
                        "| \"eee |",
                        "\\ ee\"  |",
                        "+------+",
                        NEWLINE));
    }

    @Test
    void wrapStringContentWithTwoColumns() {
        // GIVEN
        Result result = mockResult(
                asList("c1", "c2"),
                "a",
                "b",
                "aa",
                "bb",
                "aaa",
                "b",
                "a",
                "bbb",
                "aaaa",
                "bb",
                "aa",
                "bbbb",
                "aaaaa",
                "bbbbb");
        // WHEN
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(true, 2).formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        String table = printer.result();
        // THEN
        assertThat(table)
                .isEqualTo(String.join(
                        NEWLINE,
                        "+-------------+",
                        "| c1   | c2   |",
                        "+-------------+",
                        "| \"a\"  | \"b\"  |",
                        "| \"aa\" | \"bb\" |",
                        "| \"aaa | \"b\"  |",
                        "\\ \"    |      |",
                        "| \"a\"  | \"bbb |",
                        "|      \\ \"    |",
                        "| \"aaa | \"bb\" |",
                        "\\ a\"   |      |",
                        "| \"aa\" | \"bbb |",
                        "|      \\ b\"   |",
                        "| \"aaa | \"bbb |",
                        "\\ aa\"  \\ bb\"  |",
                        "+-------------+",
                        NEWLINE));
    }

    @Test
    void wrapNumberContentWithLongSize() {
        // GIVEN
        Result result = mockResult(asList("c1"), 345, 12, 978623, 132456798, 9223372036854775807L);
        // WHEN
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(true, 2).formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        String table = printer.result();
        // THEN
        assertThat(table)
                .isEqualTo(String.join(
                        NEWLINE,
                        "+---------------------+",
                        "| c1                  |",
                        "+---------------------+",
                        "| 345                 |",
                        "| 12                  |",
                        "| 978623              |",
                        "| 132456798           |",
                        "| 9223372036854775807 |",
                        "+---------------------+",
                        NEWLINE));
    }

    @Test
    void truncateContent() {
        // GIVEN
        Result result = mockResult(asList("c1"), "a", "bb", "ccc", "dddd", "eeeee");
        // WHEN
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(false, 2).formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        String table = printer.result();
        // THEN
        assertThat(table)
                .isEqualTo(String.join(
                        NEWLINE,
                        "+------+",
                        "| c1   |",
                        "+------+",
                        "| \"a\"  |",
                        "| \"bb\" |",
                        "| \"cc… |",
                        "| \"dd… |",
                        "| \"ee… |",
                        "+------+",
                        NEWLINE));
    }

    @Test
    void formatCollections() {
        // GIVEN
        Result result = mockResult(
                asList("a", "b", "c"), singletonMap("a", 42), asList(12, 13), singletonMap("a", asList(14, 15)));
        // WHEN
        String table = formatResult(result);
        // THEN
        assertThat(table).contains("| {a: 42} | [12, 13] | {a: [14, 15]} |");
    }

    @Test
    void formatEntities() {
        // GIVEN
        Map<String, Value> properties = singletonMap("name", Values.value("Mark"));
        Map<String, Value> relProperties = singletonMap("since", Values.value(2016));
        InternalNode node = new InternalNode(12, asList("Person"), properties);
        InternalRelationship relationship = new InternalRelationship(24, 12, 12, "TEST", relProperties);
        Result result =
                mockResult(asList("a", "b", "c"), node, relationship, new InternalPath(node, relationship, node));
        // WHEN
        String table = formatResult(result);
        // THEN
        assertThat(table).contains("| (:Person {name: \"Mark\"}) | [:TEST {since: 2016}] |");
        assertThat(table).contains("| (:Person {name: \"Mark\"})-[:TEST {since: 2016}]->(:Person {name: \"Mark\"}) |");
    }

    @Test
    void wrapUnicodeContent() {
        // GIVEN
        Result result = mockResult(asList("c1"), "xx", "😅😅", "🐞🐞🐞🐞🐞🐞");
        // WHEN
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(true, 1).formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        String table = printer.result();
        // THEN
        assertThat(table)
                .isEqualTo(String.join(
                        NEWLINE,
                        "+------+",
                        "| c1   |",
                        "+------+",
                        "| \"xx\" |",
                        "| \"😅😅\" |",
                        "| \"🐞🐞🐞 |",
                        "\\ 🐞🐞🐞\" |",
                        "+------+",
                        NEWLINE));
    }

    @Test
    void truncateUnicodeContent() {
        // GIVEN
        Result result = mockResult(asList("c1"), "xx", "😅😅", "🐞🐞🐞🐞🐞🐞");
        // WHEN
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(false, 1).formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        String table = printer.result();
        // THEN
        assertThat(table)
                .isEqualTo(String.join(
                        NEWLINE,
                        "+------+",
                        "| c1   |",
                        "+------+",
                        "| \"xx\" |",
                        "| \"😅😅\" |",
                        "| \"🐞🐞… |",
                        "+------+",
                        NEWLINE));
    }

    @Test
    void wrapMultilineStringContent() {
        // GIVEN
        Result result = mockResult(asList("c1"), "xxxxx", "1\n2", "1234567", "123456\n7", "1234567\n12345678");
        // WHEN
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(true, 1).formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        String table = printer.result();
        // THEN
        assertThat(table)
                .isEqualTo(String.join(
                        NEWLINE,
                        "+---------+",
                        "| c1      |",
                        "+---------+",
                        "| \"xxxxx\" |",
                        "| \"1      |",
                        "\\ 2\"      |",
                        "| \"123456 |",
                        "\\ 7\"      |",
                        "| \"123456 |",
                        "\\ 7\"      |",
                        "| \"123456 |",
                        "\\ 7       |",
                        "\\ 1234567 |",
                        "\\ 8\"      |",
                        "+---------+",
                        NEWLINE));
    }

    @Test
    void truncateMultilineStringContent() {
        // GIVEN
        Result result = mockResult(asList("c1"), "xxxxx", "1\n2", "1234567", "123456\n7", "1234567\n12345678");
        // WHEN
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(false, 1).formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        String table = printer.result();
        // THEN
        assertThat(table)
                .isEqualTo(String.join(
                        NEWLINE,
                        "+---------+",
                        "| c1      |",
                        "+---------+",
                        "| \"xxxxx\" |",
                        "| \"1…     |",
                        "| \"12345… |",
                        "| \"12345… |",
                        "| \"12345… |",
                        "+---------+",
                        NEWLINE));
    }

    @Test
    void wrapMultilineStringContentWithEmptyLines() {
        // GIVEN
        Result result = mockResult(asList("c1"), "xxxxx", "1\n2\n3\n\n4", "1\n");
        // WHEN
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(true, 1).formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        String table = printer.result();
        // THEN
        assertThat(table)
                .isEqualTo(String.join(
                        NEWLINE,
                        "+---------+",
                        "| c1      |",
                        "+---------+",
                        "| \"xxxxx\" |",
                        "| \"1      |",
                        "\\ 2       |",
                        "\\ 3       |",
                        "\\         |",
                        "\\ 4\"      |",
                        "| \"1      |",
                        "\\ \"       |",
                        "+---------+",
                        NEWLINE));
    }

    @Test
    void useServerRenderedPlanIfThere() {
        // given
        var formatter = new TableOutputFormatter(true, 1);
        var summary = mock(ResultSummary.class);
        var plan = mock(Plan.class);
        when(summary.hasPlan()).thenReturn(true);
        when(plan.arguments()).thenReturn(Map.of("string-representation", new StringValue("SERVER SIDE PLAN")));
        when(summary.plan()).thenReturn(plan);

        // when
        var formatted = formatter.formatPlan(summary);

        // then
        assertThat(formatted).isEqualTo("SERVER SIDE PLAN");
    }

    @Test
    void printWithHeading() {
        final var result = mockResult(asList("c1", "c2"), "aaaaaaa", 42, "b", 43);

        assertThat(formatResultWithHeading(result, "My Table").replace("\r\n", "\n"))
                .isEqualTo(
                        """
                        +----------------+
                        | My Table       |
                        +----------------+
                        | c1        | c2 |
                        +----------------+
                        | "aaaaaaa" | 42 |
                        | "b"       | 43 |
                        +----------------+

                        """);

        assertThat(formatResultWithHeading(result, "Long long long, so very long, heading")
                        .replace("\r\n", "\n"))
                .isEqualTo(
                        """
                        +------------------------------------------+
                        | Long long long, so very long, heading    |
                        +------------------------------------------+
                        | c1                                  | c2 |
                        +------------------------------------------+
                        | "aaaaaaa"                           | 42 |
                        | "b"                                 | 43 |
                        +------------------------------------------+

                        """);
    }

    @Test
    void formatNotifications() {
        final var notifications = List.of(
                notification("code1", "desc1", "INFORMATION"),
                notification("code1", "desc1", "INFORMATION"),
                notification("code2", "desc2", "WARNING"));

        assertThat(formatNotifications(notifications))
                .isEqualTo("""

            info: desc1 (code1)

            warn: desc2 (code2)
            """);
    }

    @Test
    void formatEmptyNotifications() {
        assertThat(formatNotifications(List.of())).isEmpty();
    }

    private static String formatResult(Result result) {
        ToStringLinePrinter printer = new ToStringLinePrinter();
        new TableOutputFormatter(true, 1000)
                .formatAndCount(new ListBoltResult(result.list(), result.consume()), printer);
        return printer.result();
    }

    private static String formatNotifications(List<Notification> notifications) {
        return new TableOutputFormatter(true, 1000).formatNotifications(notifications);
    }

    private static String formatResultWithHeading(Result result, String heading) {
        final var printer = new ToStringLinePrinter();
        new TableOutputFormatter(true, 1000)
                .formatWithHeading(new ListBoltResult(result.list(), result.consume()), printer, heading);
        return printer.result();
    }

    private static Result mockResult(List<String> cols, Object... data) {
        return mockResultWithNotification(cols, asList(data), List.of());
    }

    private static Result mockResultWithNotification(
            List<String> cols, List<Object> input, List<Notification> notifications) {
        Result result = mock(Result.class);
        Query query = mock(Query.class);
        ResultSummary summary = mock(ResultSummary.class);
        when(summary.query()).thenReturn(query);
        when(summary.notifications()).thenReturn(notifications);
        when(result.keys()).thenReturn(cols);
        List<Record> records = new ArrayList<>();
        int width = cols.size();
        for (int row = 0; row < input.size() / width; row++) {
            records.add(record(cols, input.subList(row * width, (row + 1) * width)));
        }
        when(result.list()).thenReturn(records);
        when(result.consume()).thenReturn(summary);
        when(result.consume()).thenReturn(summary);
        return result;
    }

    private Notification notification(String code, String description, String severity) {
        final var n = mock(Notification.class);
        when(n.code()).thenReturn(code);
        when(n.description()).thenReturn(description);
        when(n.rawSeverityLevel()).thenReturn(Optional.of(severity));
        return n;
    }

    private static Record record(List<String> cols, List<Object> data) {
        assert cols.size() == data.size();
        Value[] values = data.stream().map(Values::value).toArray(Value[]::new);
        return new InternalRecord(cols, values);
    }

    class ThrowAfterN implements Iterator<Record> {
        int counter = 0;
        int n;
        Record record = new InternalRecord(List.of("i"), new IntegerValue[] {new IntegerValue(1)});

        public ThrowAfterN(int n) {
            this.n = n;
        }

        @Override
        public boolean hasNext() {
            return counter <= n;
        }

        @Override
        public Record next() {
            if (counter++ < n) {
                return record;
            } else {
                throw new RuntimeException("Dummy Exception. ThrowAfter: " + counter);
            }
        }

        @Override
        public void remove() {
            Iterator.super.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super Record> action) {
            Iterator.super.forEachRemaining(action);
        }
    }
}
