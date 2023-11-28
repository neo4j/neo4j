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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.internal.helpers.NameUtil.escapeName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.shell.state.BoltResult;
import org.neo4j.values.storable.DurationValue;

public interface OutputFormatter {

    String COMMA_SEPARATOR = ", ";
    String COLON_SEPARATOR = ": ";
    String COLON = ":";
    String SPACE = " ";
    String NEWLINE = System.getProperty("line.separator");
    List<String> INFO_SUMMARY = asList("Version", "Planner", "Runtime");

    static String collectNodeLabels(Node node) {
        StringBuilder sb = new StringBuilder();
        node.labels().forEach(label -> sb.append(COLON).append(escapeName(label)));
        return sb.toString();
    }

    static String listAsString(List<String> list) {
        return list.stream().collect(Collectors.joining(COMMA_SEPARATOR, "[", "]"));
    }

    static String mapAsStringWithEmpty(Map<String, Object> map) {
        return map.isEmpty() ? "" : mapAsString(map);
    }

    static String mapAsString(Map<String, Object> map) {
        return map.entrySet().stream()
                .map(e -> escapeName(e.getKey()) + COLON_SEPARATOR + e.getValue())
                .collect(Collectors.joining(COMMA_SEPARATOR, "{", "}"));
    }

    static String joinWithSpace(List<String> strings) {
        return strings.stream().filter(OutputFormatter::isNotBlank).collect(Collectors.joining(SPACE));
    }

    static String joinNonBlanks(String delim, List<String> strings) {
        return strings.stream().filter(OutputFormatter::isNotBlank).collect(Collectors.joining(delim));
    }

    static boolean isNotBlank(String string) {
        return string != null && !string.trim().isEmpty();
    }

    static char[] repeat(char c, int times) {
        char[] chars = new char[times];
        Arrays.fill(chars, c);
        return chars;
    }

    static String repeat(String c, int times) {
        StringBuilder sb = new StringBuilder(times * c.length());
        for (int i = 0; i < times; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    static Map<String, Value> info(ResultSummary summary) {
        Map<String, Value> result = new LinkedHashMap<>();
        if (!summary.hasPlan()) {
            return result;
        }

        Plan plan = summary.plan();
        result.put("Plan", Values.value(summary.hasProfile() ? "PROFILE" : "EXPLAIN"));
        result.put("Statement", Values.value(summary.queryType().name()));
        Map<String, Value> arguments = plan.arguments();
        Value emptyString = Values.value("");
        Value questionMark = Values.value("?");

        for (String key : INFO_SUMMARY) {
            Value value =
                    arguments.getOrDefault(key, arguments.getOrDefault(key.toLowerCase(Locale.ROOT), emptyString));
            result.put(key, value);
        }
        result.put(
                "Time",
                Values.value(summary.resultAvailableAfter(MILLISECONDS) + summary.resultConsumedAfter(MILLISECONDS)));
        if (summary.hasProfile()) {
            result.put("DbHits", Values.value(collectHits(summary.profile())));
        }
        if (summary.hasProfile()) {
            result.put("Rows", Values.value(summary.profile().records()));
        }
        if (summary.hasProfile()) {
            result.put("Memory (Bytes)", arguments.getOrDefault("GlobalMemory", questionMark));
        }
        return result;
    }

    static long collectHits(ProfiledPlan operator) {
        long hits = operator.dbHits();
        hits = operator.children().stream().map(OutputFormatter::collectHits).reduce(hits, Long::sum);
        return hits;
    }

    int formatAndCount(BoltResult result, LinePrinter linePrinter);

    default String formatValue(final Value value) {
        if (value == null) {
            return "";
        }
        TypeRepresentation type = (TypeRepresentation) value.type();
        switch (type.constructor()) {
            case LIST:
                return listAsString(value.asList(this::formatValue));
            case MAP:
                return mapAsString(value.asMap(this::formatValue));
            case NODE:
                return nodeAsString(value.asNode());
            case RELATIONSHIP:
                return relationshipAsString(value.asRelationship());
            case PATH:
                return pathAsString(value.asPath());
            case POINT:
                return pointAsString(value.asPoint());
            case DURATION:
                return DurationValue.parse(value.toString()).prettyPrint();
            case ANY:
            case BOOLEAN:
            case BYTES:
            case STRING:
            case NUMBER:
            case INTEGER:
            case FLOAT:
            case DATE:
            case TIME:
            case DATE_TIME:
            case LOCAL_TIME:
            case LOCAL_DATE_TIME:
            case NULL:
            default:
                return value.toString();
        }
    }

    default String pointAsString(Point point) {
        StringBuilder stringBuilder = new StringBuilder("point({");
        stringBuilder.append("srid:").append(point.srid()).append(",");
        stringBuilder.append(" x:").append(point.x()).append(",");
        stringBuilder.append(" y:").append(point.y());
        double z = point.z();
        if (!Double.isNaN(z)) {
            stringBuilder.append(", z:").append(z);
        }
        stringBuilder.append("})");
        return stringBuilder.toString();
    }

    default String pathAsString(Path path) {
        List<String> list = new ArrayList<>(path.length());
        Node lastTraversed = path.start();
        if (lastTraversed != null) {
            list.add(nodeAsString(lastTraversed));

            for (Path.Segment segment : path) {
                Relationship relationship = segment.relationship();
                if (relationship.startNodeId() == lastTraversed.id()) {
                    list.add("-" + relationshipAsString(relationship) + "->");
                } else {
                    list.add("<-" + relationshipAsString(relationship) + "-");
                }
                list.add(nodeAsString(segment.end()));
                lastTraversed = segment.end();
            }
        }

        return String.join("", list);
    }

    default String relationshipAsString(Relationship relationship) {
        List<String> relationshipAsString = new ArrayList<>();
        relationshipAsString.add(COLON + escapeName(relationship.type()));
        relationshipAsString.add(mapAsStringWithEmpty(relationship.asMap(this::formatValue)));

        return "[" + joinWithSpace(relationshipAsString) + "]";
    }

    default String nodeAsString(final Node node) {
        List<String> nodeAsString = new ArrayList<>();
        nodeAsString.add(collectNodeLabels(node));
        nodeAsString.add(mapAsStringWithEmpty(node.asMap(this::formatValue)));

        return "(" + joinWithSpace(nodeAsString) + ")";
    }

    default String formatPlan(ResultSummary summary) {
        return "";
    }

    default String formatInfo(ResultSummary summary) {
        return "";
    }

    default String formatFooter(BoltResult result, int numberOfRows) {
        return "";
    }

    default String formatNotifications(List<Notification> notifications) {
        return "";
    }

    Set<Capabilities> capabilities();

    enum Capabilities {
        INFO,
        PLAN,
        RESULT,
        FOOTER,
        STATISTICS,
        NOTIFICATIONS
    }
}
