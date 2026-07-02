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
package org.neo4j.internal.schema;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.NameUtil.escapeBackticks;
import static org.neo4j.internal.helpers.NameUtil.forceEscapeName;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.NameUtil;
import org.neo4j.internal.helpers.Strings;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.schema.SettingsAccessor.IndexConfigAccessor;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.Value;
import org.neo4j.values.utils.PrettyPrinter;

public class SchemaStatementCreator {
    static final String CREATE_NODE_LABEL_INDEX = "CREATE LOOKUP INDEX `%s` FOR (n) ON EACH labels(n)";
    static final String CREATE_RELATIONSHIP_TYPE_INDEX = "CREATE LOOKUP INDEX `%s` FOR ()-[r]-() ON EACH type(r)";
    public static final String CREATE_NODE_PROPERTY_INDEX = "CREATE %s INDEX `%s` FOR (n:`%s`) ON (%s) OPTIONS %s";
    public static final String CREATE_RELATIONSHIP_PROPERTY_INDEX =
            "CREATE %s INDEX `%s` FOR ()-[r:`%s`]-() ON (%s) OPTIONS %s";
    public static final String CREATE_NODE_FULLTEXT_INDEX =
            "CREATE FULLTEXT INDEX `%s` FOR (n:%s) ON EACH [%s] OPTIONS %s";
    public static final String CREATE_RELATIONSHIP_FULLTEXT_INDEX =
            "CREATE FULLTEXT INDEX `%s` FOR ()-[r:%s]-() ON EACH [%s] OPTIONS %s";
    public static final String CREATE_NODE_VECTOR_INDEX =
            "CREATE VECTOR INDEX `%s` FOR (n:%s) ON (n.`%s`)%s OPTIONS %s";
    public static final String CREATE_RELATIONSHIP_VECTOR_INDEX =
            "CREATE VECTOR INDEX `%s` FOR ()-[r:%s]-() ON (r.`%s`)%s OPTIONS %s";

    public static final String CREATE_UNIQUE_NODE_PROPERTY_CONSTRAINT =
            "CREATE CONSTRAINT `%s` FOR (n:`%s`) REQUIRE (%s) IS UNIQUE OPTIONS %s";
    static final String CREATE_UNIQUE_RELATIONSHIP_PROPERTY_CONSTRAINT =
            "CREATE CONSTRAINT `%s` FOR ()-[r:`%s`]-() REQUIRE (%s) IS UNIQUE OPTIONS %s";
    public static final String CREATE_NODE_KEY_CONSTRAINT =
            "CREATE CONSTRAINT `%s` FOR (n:`%s`) REQUIRE (%s) IS NODE KEY OPTIONS %s";
    static final String CREATE_RELATIONSHIP_KEY_CONSTRAINT =
            "CREATE CONSTRAINT `%s` FOR ()-[r:`%s`]-() REQUIRE (%s) IS RELATIONSHIP KEY OPTIONS %s";
    public static final String CREATE_NODE_EXISTENCE_CONSTRAINT =
            "CREATE CONSTRAINT `%s` FOR (n:`%s`) REQUIRE (n.`%s`) IS NOT NULL";
    public static final String CREATE_RELATIONSHIP_EXISTENCE_CONSTRAINT =
            "CREATE CONSTRAINT `%s` FOR ()-[r:`%s`]-() REQUIRE (r.`%s`) IS NOT NULL";
    static final String CREATE_NODE_PROPERTY_TYPE_CONSTRAINT =
            "CREATE CONSTRAINT `%s` FOR (n:`%s`) REQUIRE (n.`%s`) IS :: %s";
    static final String CREATE_RELATIONSHIP_PROPERTY_TYPE_CONSTRAINT =
            "CREATE CONSTRAINT `%s` FOR ()-[r:`%s`]-() REQUIRE (r.`%s`) IS :: %s";

    static final String DROP_INDEX_IF_EXISTS = "DROP INDEX `%s` IF EXISTS";

    static final String DROP_CONSTRAINT_IF_EXISTS = "DROP CONSTRAINT `%s` IF EXISTS";

    public static String getIndexSchemaStatement(TokenRead tokenRead, IndexDescriptor index) throws KernelException {
        return getIndexSchemaStatement(tokenRead, index, false);
    }

    public static String getIndexSchemaStatement(TokenRead tokenRead, IndexDescriptor index, boolean withIndexProvider)
            throws KernelException {
        String name = escapeBackticks(index.getName());
        SchemaDescriptor schema = index.schema();
        boolean isNodeIndex = schema.entityType() == EntityType.NODE;
        IndexType indexType = index.getIndexType();
        IndexProviderDescriptor indexProvider = index.getIndexProvider();
        IndexConfig indexConfig = index.getIndexConfig();

        String createStatement =
                switch (indexType) {
                    case LOOKUP -> {
                        String createStatementFormat =
                                isNodeIndex ? CREATE_NODE_LABEL_INDEX : CREATE_RELATIONSHIP_TYPE_INDEX;
                        yield createStatementFormat.formatted(name);
                    }
                    case RANGE, TEXT, POINT -> {
                        String createStatementFormat =
                                isNodeIndex ? CREATE_NODE_PROPERTY_INDEX : CREATE_RELATIONSHIP_PROPERTY_INDEX;
                        yield createStatementFormat.formatted(
                                indexType,
                                name,
                                labelOrRelType(tokenRead, schema),
                                propertiesListWithPrefix(schema, tokenRead),
                                withIndexProvider ? options(indexProvider, indexConfig) : options(indexConfig));
                    }
                    case FULLTEXT -> {
                        String createStatementFormat =
                                isNodeIndex ? CREATE_NODE_FULLTEXT_INDEX : CREATE_RELATIONSHIP_FULLTEXT_INDEX;
                        yield createStatementFormat.formatted(
                                name,
                                labelsOrRelTypesAsOrList(tokenRead, schema),
                                propertiesListWithPrefix(schema, tokenRead),
                                withIndexProvider ? options(indexProvider, indexConfig) : options(indexConfig));
                    }
                    case VECTOR -> {
                        String createStatementFormat =
                                isNodeIndex ? CREATE_NODE_VECTOR_INDEX : CREATE_RELATIONSHIP_VECTOR_INDEX;

                        int[] propertyIds = schema.getPropertyIds();
                        String vectorKey = escapeBackticks(tokenRead.propertyKeyName(propertyIds[0]));
                        int[] additionalPropertyIds = Arrays.copyOfRange(propertyIds, 1, propertyIds.length);
                        String additionalPropertyKeys = additionalPropertyIds.length > 0
                                ? " WITH ["
                                        + propertiesListWithPrefix(
                                                schema.entityType(), tokenRead, additionalPropertyIds)
                                        + "]"
                                : Strings.EMPTY;

                        TypedIndexSettingsValidator<VectorIndexConfig> settingsValidator =
                                VectorIndexVersion.fromDescriptor(indexProvider).indexSettingValidator();
                        VectorIndexConfig vectorIndexConfig = settingsValidator.interpretAuthoritativeToTypedConfig(
                                new IndexConfigAccessor(indexConfig));
                        IndexConfig validatedConfig = vectorIndexConfig.config();
                        yield createStatementFormat.formatted(
                                name,
                                labelsOrRelTypesAsOrList(tokenRead, schema),
                                vectorKey,
                                additionalPropertyKeys,
                                withIndexProvider ? options(indexProvider, validatedConfig) : options(validatedConfig));
                    }
                };

        return withIndexProvider ? "CYPHER 5 " + createStatement : createStatement;
    }

    public static String getConstraintSchemaStatement(
            Function<String, IndexDescriptor> indexLookup, TokenRead tokenRead, ConstraintDescriptor constraint)
            throws KernelException {
        String rawName = constraint.getName();
        String name = escapeBackticks(constraint.getName());
        String labelOrRelType = labelOrRelType(tokenRead, constraint.schema());
        if (constraint.isIndexBackedConstraint()) {
            String properties = propertiesListWithPrefix(constraint.schema(), tokenRead);
            IndexDescriptor backingIndex = indexLookup.apply(rawName);
            String options = options(backingIndex.getIndexConfig());
            if (constraint.isNodeUniquenessConstraint()) {
                return format(CREATE_UNIQUE_NODE_PROPERTY_CONSTRAINT, name, labelOrRelType, properties, options);
            }
            if (constraint.isNodeKeyConstraint()) {
                return format(CREATE_NODE_KEY_CONSTRAINT, name, labelOrRelType, properties, options);
            }
            if (constraint.isRelationshipUniquenessConstraint()) {
                return format(
                        CREATE_UNIQUE_RELATIONSHIP_PROPERTY_CONSTRAINT, name, labelOrRelType, properties, options);
            }
            if (constraint.isRelationshipKeyConstraint()) {
                return format(CREATE_RELATIONSHIP_KEY_CONSTRAINT, name, labelOrRelType, properties, options);
            }
        }

        int propertyId = constraint.schema().getPropertyId();
        String property = escapeBackticks(tokenRead.propertyKeyName(propertyId));
        if (constraint.isPropertyTypeConstraint()) {
            String formatString = constraint.schema().isLabelSchemaDescriptor()
                    ? CREATE_NODE_PROPERTY_TYPE_CONSTRAINT
                    : CREATE_RELATIONSHIP_PROPERTY_TYPE_CONSTRAINT;
            return format(
                    formatString,
                    name,
                    labelOrRelType,
                    property,
                    constraint.asPropertyTypeConstraint().propertyType().userDescription());
        }
        if (constraint.isNodePropertyExistenceConstraint()) {
            return format(CREATE_NODE_EXISTENCE_CONSTRAINT, name, labelOrRelType, property);
        }
        if (constraint.isRelationshipPropertyExistenceConstraint()) {
            return format(CREATE_RELATIONSHIP_EXISTENCE_CONSTRAINT, name, labelOrRelType, property);
        }
        throw new IllegalArgumentException("Did not recognize constraint type " + constraint);
    }

    public static String getIndexDropSchemaStatement(IndexDescriptor index) {
        return DROP_INDEX_IF_EXISTS.formatted(escapeBackticks(index.getName()));
    }

    public static String getConstraintDropSchemaStatement(ConstraintDescriptor constraint) {
        return DROP_CONSTRAINT_IF_EXISTS.formatted(escapeBackticks(constraint.getName()));
    }

    // For indexes and constraint with only one expected label/type.
    public static String labelOrRelType(TokenRead tokenRead, SchemaDescriptor schema) throws KernelException {
        if (EntityType.NODE.equals(schema.entityType())) {
            return escapeBackticks(tokenRead.nodeLabelName(schema.getLabelId()));
        }
        return escapeBackticks(tokenRead.relationshipTypeName(schema.getRelTypeId()));
    }

    // For fulltext index that wants labels/types differently.
    public static String labelsOrRelTypesAsOrList(TokenRead tokenRead, SchemaDescriptor schema) throws KernelException {
        StringJoiner labelsOrRelTypes = new StringJoiner("|");
        for (int entityTokenId : schema.getEntityTokenIds()) {
            if (EntityType.NODE.equals(schema.entityType())) {
                labelsOrRelTypes.add(forceEscapeName(tokenRead.nodeLabelName(entityTokenId)));
            } else {
                labelsOrRelTypes.add(forceEscapeName(tokenRead.relationshipTypeName(entityTokenId)));
            }
        }
        return labelsOrRelTypes.toString();
    }

    public static String options(IndexProviderDescriptor providerDescriptor, IndexConfig indexConfig) {
        Set<Entry<String, Value>> entries = indexConfig.entries();
        PrettyPrinter pp = new PrettyPrinter("'", AnyValueWriter.EntityMode.FULL);
        pp.beginMap(entries.isEmpty() ? 1 : 2);

        pp.writeString("indexProvider");
        pp.writeString(NameUtil.escapeSingleQuotes(providerDescriptor.name()));
        writeIndexConfig(entries, pp);
        pp.endMap();
        return pp.value();
    }

    public static String options(IndexConfig indexConfig) {
        Set<Entry<String, Value>> entries = indexConfig.entries();
        PrettyPrinter pp = new PrettyPrinter("'", AnyValueWriter.EntityMode.FULL);
        pp.beginMap(entries.isEmpty() ? 1 : 2);
        writeIndexConfig(entries, pp);
        pp.endMap();
        return pp.value();
    }

    private static void writeIndexConfig(Collection<Entry<String, Value>> entries, PrettyPrinter pp) {
        if (!entries.isEmpty()) {
            pp.writeString("indexConfig");
            pp.beginMap(entries.size());
            entries.forEach(entry -> {
                pp.writeString(NameUtil.forceEscapeName(entry.getKey()));
                entry.getValue().writeTo(pp);
            });
            pp.endMap();
        }
    }

    public static String propertiesListWithPrefix(SchemaDescriptor schema, TokenRead tokenRead)
            throws PropertyKeyIdNotFoundKernelException {
        return propertiesListWithPrefix(schema.entityType(), tokenRead, schema.getPropertyIds());
    }

    public static String propertiesListWithPrefix(EntityType entityType, TokenRead tokenRead, int... propertyIds)
            throws PropertyKeyIdNotFoundKernelException {
        StringJoiner joiner = propStringJoiner(entityType);
        for (int propertyId : propertyIds) {
            joiner.add(escapeBackticks(tokenRead.propertyKeyName(propertyId)));
        }
        return joiner.toString();
    }

    private static StringJoiner propStringJoiner(EntityType entityType) {
        if (EntityType.NODE.equals(entityType)) {
            return new StringJoiner("`, n.`", "n.`", "`");
        }
        return new StringJoiner("`, r.`", "r.`", "`");
    }
}
