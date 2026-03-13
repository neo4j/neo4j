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

package org.neo4j.fleetmanagement.procedures;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.fleetmanagement.common.ValuesDocumentation;
import org.neo4j.fleetmanagement.communication.model.ConnectMessage;
import org.neo4j.fleetmanagement.communication.model.MetricsMessage;
import org.neo4j.fleetmanagement.communication.model.Neo4jConfigMessage;
import org.neo4j.fleetmanagement.communication.model.PingMessage;
import org.neo4j.fleetmanagement.communication.model.QueryReportMessage;
import org.neo4j.fleetmanagement.communication.model.ReportingMessage;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

public class Documentation {
    public static class DocumentationResult {
        @Description("Name of the message in which the current field appears.")
        public String messageType;

        @Description("The path to the field.")
        public String fieldPath;

        @Description("Description of the field.")
        public String description;

        @Description("The data type of this field.")
        public String fieldType;

        @Description("The current values of fields which are dynamically determined on connection.")
        public List<String> values;

        public DocumentationResult(
                String messageType, String fieldPath, String description, String fieldType, List<String> values) {
            this.messageType = messageType;
            this.fieldPath = fieldPath;
            this.description = description;
            this.fieldType = fieldType;
            this.values = values;
        }

        @Override
        public String toString() {
            return String.format(
                    "DocumentationResult{messageType='%s', fieldPath='%s', description='%s', fieldType='%s', values='%s'}",
                    messageType, fieldPath, description, fieldType, values);
        }
    }

    @Procedure(name = "fleetManagement.reportedData", mode = Mode.READ)
    @SystemProcedure
    @Description("Generate documentation for the data structures used in Fleet Manager messages.")
    public Stream<DocumentationResult> generateDocumentation() throws Exception {
        List<DocumentationResult> results = new ArrayList<>();
        documentClass(ConnectMessage.class, "ConnectMessage", "", results);
        documentClass(MetricsMessage.class, "MetricsMessage", "", results);
        documentClass(Neo4jConfigMessage.class, "Neo4jConfigMessage", "", results);
        documentClass(ReportingMessage.class, "ReportingMessage", "", results);
        documentClass(PingMessage.class, "PingMessage", "", results);
        documentClass(QueryReportMessage.class, "QueryReportMessage", "", results);
        return results.stream();
    }

    private void documentClass(Class<?> clazz, String messageType, String prefix, List<DocumentationResult> results)
            throws Exception {
        JsonClassDescription classDescription = clazz.getAnnotation(JsonClassDescription.class);
        if (classDescription != null) {
            results.add(
                    new DocumentationResult(messageType, prefix, classDescription.value(), getTypeString(clazz), null));
        }

        for (Field field : clazz.getDeclaredFields()) {
            documentField(messageType, prefix, results, field, clazz);
        }

        for (Method method : clazz.getDeclaredMethods()) {
            JsonProperty methodPropertyKey = method.getAnnotation(JsonProperty.class);
            JsonPropertyDescription methodDescription = method.getAnnotation(JsonPropertyDescription.class);
            if (methodPropertyKey != null && methodDescription != null) {
                results.add(new DocumentationResult(
                        messageType,
                        prefix + "." + methodPropertyKey.value(),
                        methodDescription.value(),
                        method.getReturnType().getSimpleName(),
                        null));
            }
        }
    }

    private void documentField(
            String messageType, String prefix, List<DocumentationResult> results, Field field, Class<?> parent)
            throws Exception {
        String fieldPath = getFieldPath(prefix, field);

        Class<?> fieldType = field.getType();
        var isRecursive = fieldType.equals(parent);

        JsonPropertyDescription fieldDescription = field.getAnnotation(JsonPropertyDescription.class);
        if (fieldDescription != null) {
            results.add(new DocumentationResult(
                    messageType,
                    fieldPath,
                    fieldDescription.value(),
                    getTypeString(field.getGenericType()) + (isRecursive ? " (recursive)" : ""),
                    getValues(field)));
        }
        if (isRecursive) {
            return;
        }

        if (fieldType.isAssignableFrom(List.class)) {
            Type firstTypeArgument = getGenericArgument(field.getGenericType(), 0);
            documentList(messageType, results, firstTypeArgument, fieldPath);
        } else if (fieldType.isAssignableFrom(Map.class)) {
            documentMap(messageType, results, getGenericArgument(field.getGenericType(), 1), fieldPath);
        } else if (isRelevant(fieldType)) {
            documentClass(fieldType, messageType, fieldPath, results);
        }
    }

    private static String getFieldPath(String prefix, Field field) {
        JsonProperty jsonProperty = field.getAnnotation(JsonProperty.class);
        String fieldName = jsonProperty == null ? field.getName() : jsonProperty.value();
        if (prefix.isEmpty()) {
            return fieldName;
        }
        return String.format("%s.%s", prefix, fieldName);
    }

    private static List<String> getValues(Field field) throws Exception {
        ValuesDocumentation valuesDocumentation = field.getAnnotation(ValuesDocumentation.class);
        if (valuesDocumentation != null) {
            return valuesDocumentation
                    .valueSupplier()
                    .getDeclaredConstructor()
                    .newInstance()
                    .get();
        }
        return null;
    }

    private static Type getGenericArgument(Type type, int index) {
        if (type instanceof ParameterizedType) {
            Type[] actualTypeArguments = ((ParameterizedType) type).getActualTypeArguments();
            if (index < actualTypeArguments.length) {
                return actualTypeArguments[index];
            }
        }
        throw new IllegalArgumentException(String.format("Cannot get type argument at index %d from %s", index, type));
    }

    private static boolean isRelevant(Class<?> clazz) {
        return !clazz.isEnum() && clazz.getPackageName().contains("fleetmanagement");
    }

    private void documentMap(String messageType, List<DocumentationResult> results, Type valueType, String fieldPath)
            throws Exception {
        if (valueType instanceof ParameterizedType) {
            Type rawValueType = ((ParameterizedType) valueType).getRawType();
            String newPath = String.format("%s<>", fieldPath);
            if (rawValueType == List.class) {
                documentList(messageType, results, getGenericArgument(valueType, 0), newPath);
            } else if (rawValueType == Map.class) {
                documentMap(messageType, results, getGenericArgument(valueType, 1), newPath);
            } else {
                throw new IllegalArgumentException(String.format("Unsupported Map value type: %s", valueType));
            }
        } else if (isRelevant(getClazz(valueType))) {
            documentClass(getClazz(valueType), messageType, fieldPath + getTypeString(valueType), results);
        }
    }

    private void documentList(String messageType, List<DocumentationResult> results, Type elementType, String fieldPath)
            throws Exception {
        documentClass(getClazz(elementType), messageType, String.format("%s[]", fieldPath), results);
    }

    private static Class<?> getClazz(Type type) {
        if (type instanceof ParameterizedType) {
            Type rawType = ((ParameterizedType) type).getRawType();
            if (rawType instanceof Class<?>) {
                return (Class<?>) rawType;
            } else {
                return getClazz(rawType);
            }
        } else {
            return (Class<?>) type;
        }
    }

    private static String getTypeString(Type type) {
        if (type instanceof ParameterizedType) {
            Type rawValueType = ((ParameterizedType) type).getRawType();
            if (Collection.class.isAssignableFrom((Class<?>) rawValueType)) {
                return String.format("Collection<%s>", getTypeString(getGenericArgument(type, 0)));
            } else if (Map.class.isAssignableFrom((Class<?>) rawValueType)) {
                return String.format(
                        "Map<%s, %s>",
                        getTypeString(getGenericArgument(type, 0)), getTypeString(getGenericArgument(type, 1)));
            } else {
                throw new IllegalArgumentException(String.format("Unsupported type: %s", type));
            }
        } else {
            Class<?> clazz = (Class<?>) type;
            if (clazz.isArray()) {
                return String.format("%s[]", getTypeString(clazz.getComponentType()));
            } else if (clazz.isEnum()) {
                return "String";
            } else {
                return clazz.getSimpleName();
            }
        }
    }
}
