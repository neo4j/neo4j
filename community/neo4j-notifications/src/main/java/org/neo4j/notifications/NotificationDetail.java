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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.IndexHintException;
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType;

public class NotificationDetail {

    public static String commaSeparated(final Iterable<String> values) {
        return String.join(", ", values);
    }

    public static String deprecatedName(final String oldName) {
        return ": `" + oldName + "`.";
    }

    public static String deprecatedName(final String oldName, final String newName) {
        return ". ('" + oldName + "' has been replaced by '" + newName + "')";
    }

    public static String index(
            final IndexHintIndexType indexType, final String label, final List<String> propertyKeys) {
        final var prettyProperties = commaSeparated(propertyKeys);
        final var prettyLabel = labelOrRelationshipType(label);
        final var prettyIndexHint =
                switch (indexType) {
                    case TEXT -> "TEXT INDEX";
                    case RANGE -> "RANGE INDEX";
                    case POINT -> "POINT INDEX";
                    default -> "INDEX";
                };
        return prettyIndexHint + " " + prettyLabel + "(" + prettyProperties + ")";
    }

    public static String indexHint(
            final EntityType entityType,
            final IndexHintIndexType indexType,
            final String variableName,
            final String labelName,
            final String... propertyKeyNames) {
        String indexFormatString = IndexHintException.indexFormatString(
                variableName, labelName, Arrays.asList(propertyKeyNames), entityType, indexType);
        return createNotificationDetail("index", indexFormatString, true);
    }

    public static String missingLabel(final String labelName) {
        return createNotificationDetail("the missing label name", labelName, true);
    }

    public static String missingRelationshipType(final String relType) {
        return createNotificationDetail("the missing relationship type", relType, true);
    }

    public static String missingParameters(final List<String> parameters) {
        return "Missing parameters: " + commaSeparated(parameters);
    }

    public static String procedureWarning(final String procedure, final String warning) {
        return String.format(warning, procedure);
    }

    public static String propertyName(final String name) {
        return createNotificationDetail("the missing property name", name, true);
    }

    public static String shadowingVariable(final String name) {
        return createNotificationDetail("the shadowing variable", name, true);
    }

    public static String repeatedVarLengthRel(final String name) {
        return createNotificationDetail("the repeated variable-length relationship", name, true);
    }

    public static String joinKey(List<String> identifiers) {
        boolean singular = identifiers.size() == 1;
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String identifier : identifiers) {
            if (first) {
                first = false;
            } else {
                builder.append(", ");
            }
            builder.append(identifier);
        }
        return createNotificationDetail(
                singular ? "hinted join key identifier" : "hinted join key identifiers", builder.toString(), singular);
    }

    public static String cartesianProductDescription(Set<String> identifiers) {
        return createNotificationDetail(identifiers, "identifier", "identifiers");
    }

    public static String nodeIndexSeekOrScan(Set<String> labels) {
        final var prettyLabels =
                labels.stream().map(NotificationDetail::labelOrRelationshipType).collect(Collectors.toSet());
        return createNotificationDetail(prettyLabels, "indexed label", "indexed labels");
    }

    public static String relationshipIndexSeekOrScan(Set<String> labels) {
        final var prettyLabels =
                labels.stream().map(NotificationDetail::labelOrRelationshipType).collect(Collectors.toSet());
        return createNotificationDetail(prettyLabels, "indexed relationship type", "indexed relationship types");
    }

    public static String deprecatedField(final String procedure, final String field) {
        return "'" + field + "' returned by '" + procedure + "' is deprecated.";
    }

    public static String deprecatedInputField(final String procedureOrFunction, final String field) {
        return "'" + field + "' used by '" + procedureOrFunction + "' is deprecated.";
    }

    private static String createNotificationDetail(Set<String> elements, String singularTerm, String pluralTerm) {
        StringBuilder builder = new StringBuilder();
        builder.append('(');
        String separator = "";
        for (String element : elements) {
            builder.append(separator);
            builder.append(element);
            separator = ", ";
        }
        builder.append(')');
        boolean singular = elements.size() == 1;
        return createNotificationDetail(singular ? singularTerm : pluralTerm, builder.toString(), singular);
    }

    private static String createNotificationDetail(final String name, final String value, final boolean singular) {
        return name + " " + (singular ? "is:" : "are:") + " " + value;
    }

    private static String labelOrRelationshipType(final String labelName) {
        return ":" + labelName;
    }

    public static String deprecationNotificationDetail(final String replacement) {
        return "Please use '" + replacement + "' instead";
    }

    public static String unsatisfiableRelTypeExpression(String expression) {
        return "`" + expression
                + "` can never be satisfied by any relationship. Relationships must have exactly one relationship type.";
    }

    public static String repeatedRelationship(String relationshipName) {
        return "Relationship `" + relationshipName + "` was repeated";
    }
}
