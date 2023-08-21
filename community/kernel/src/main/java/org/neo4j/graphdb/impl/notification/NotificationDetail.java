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
package org.neo4j.graphdb.impl.notification;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.IndexHintException;
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType;

public class NotificationDetail {

    public static String deprecatedName(final String oldName, final String newName) {
        if (newName == null || newName.trim().isEmpty()) {
            return String.format(": `%s`.", oldName);
        } else {
            return String.format(". ('%s' has been replaced by '%s')", oldName, newName);
        }
    }

    public static String nodeAnyIndex(
            final String variableName, final String labelName, final String... propertyKeyNames) {
        String indexFormatString = IndexHintException.indexFormatString(
                variableName, labelName, Arrays.asList(propertyKeyNames), EntityType.NODE, IndexHintIndexType.ANY);
        return createNotificationDetail("index", indexFormatString, true);
    }

    public static String nodeTextIndex(
            final String variableName, final String labelName, final String... propertyKeyNames) {
        String indexFormatString = IndexHintException.indexFormatString(
                variableName, labelName, Arrays.asList(propertyKeyNames), EntityType.NODE, IndexHintIndexType.TEXT);
        return createNotificationDetail("index", indexFormatString, true);
    }

    public static String nodeRangeIndex(
            final String variableName, final String labelName, final String... propertyKeyNames) {
        String indexFormatString = IndexHintException.indexFormatString(
                variableName, labelName, Arrays.asList(propertyKeyNames), EntityType.NODE, IndexHintIndexType.RANGE);
        return createNotificationDetail("index", indexFormatString, true);
    }

    public static String nodePointIndex(
            final String variableName, final String labelName, final String... propertyKeyNames) {
        String indexFormatString = IndexHintException.indexFormatString(
                variableName, labelName, Arrays.asList(propertyKeyNames), EntityType.NODE, IndexHintIndexType.POINT);
        return createNotificationDetail("index", indexFormatString, true);
    }

    public static String relationshipAnyIndex(
            final String variableName, final String relationshipTypeName, final String... propertyKeyNames) {
        String indexFormatString = IndexHintException.indexFormatString(
                variableName,
                relationshipTypeName,
                Arrays.asList(propertyKeyNames),
                EntityType.RELATIONSHIP,
                IndexHintIndexType.ANY);
        return createNotificationDetail("index", indexFormatString, true);
    }

    public static String relationshipTextIndex(
            final String variableName, final String relationshipTypeName, final String... propertyKeyNames) {
        String indexFormatString = IndexHintException.indexFormatString(
                variableName,
                relationshipTypeName,
                Arrays.asList(propertyKeyNames),
                EntityType.RELATIONSHIP,
                IndexHintIndexType.TEXT);
        return createNotificationDetail("index", indexFormatString, true);
    }

    public static String relationshipRangeIndex(
            final String variableName, final String relationshipTypeName, final String... propertyKeyNames) {
        String indexFormatString = IndexHintException.indexFormatString(
                variableName,
                relationshipTypeName,
                Arrays.asList(propertyKeyNames),
                EntityType.RELATIONSHIP,
                IndexHintIndexType.RANGE);
        return createNotificationDetail("index", indexFormatString, true);
    }

    public static String relationshipPointIndex(
            final String variableName, final String relationshipTypeName, final String... propertyKeyNames) {
        String indexFormatString = IndexHintException.indexFormatString(
                variableName,
                relationshipTypeName,
                Arrays.asList(propertyKeyNames),
                EntityType.RELATIONSHIP,
                IndexHintIndexType.POINT);
        return createNotificationDetail("index", indexFormatString, true);
    }

    public static String label(final String labelName) {
        return createNotificationDetail("the missing label name", labelName, true);
    }

    public static String relationshipType(final String relType) {
        return createNotificationDetail("the missing relationship type", relType, true);
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
        return createNotificationDetail(labels, "indexed label", "indexed labels");
    }

    public static String relationshipIndexSeekOrScan(Set<String> labels) {
        return createNotificationDetail(labels, "indexed relationship type", "indexed relationship types");
    }

    public static String deprecatedField(final String procedure, final String field) {
        return String.format("'%s' returned by '%s' is deprecated.", field, procedure);
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
        return String.format("%s %s %s", name, singular ? "is:" : "are:", value);
    }

    public static String deprecationNotificationDetail(final String replacement) {
        return String.format("Please use '%s' instead", replacement);
    }

    public static String unsatisfiableRelTypeExpression(String expression) {
        return String.format(
                "`%s` can never be satisfied by any relationship. Relationships must have exactly one relationship type.",
                expression);
    }

    public static String repeatedRelationship(String relationshipName) {
        return String.format("Relationship `%s` was repeated", relationshipName);
    }
}
