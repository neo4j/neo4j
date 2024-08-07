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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.RelationshipEndpointConstraintDescriptor;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.internal.schema.constraints.TypeConstraintDescriptor;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.StringArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SchemaRuleMapifier {
    private static final String PROP_SCHEMA_RULE_PREFIX = "__org.neo4j.SchemaRule.";
    private static final String PROP_SCHEMA_RULE_TYPE =
            PROP_SCHEMA_RULE_PREFIX + "schemaRuleType"; // index / constraint
    private static final String PROP_INDEX_RULE_TYPE = PROP_SCHEMA_RULE_PREFIX + "indexRuleType"; // Uniqueness
    private static final String PROP_CONSTRAINT_RULE_TYPE =
            PROP_SCHEMA_RULE_PREFIX + "constraintRuleType"; // Existence / Uniqueness / ...
    private static final String PROP_SCHEMA_GRAPH_TYPE_DEPENDENCE = PROP_SCHEMA_RULE_PREFIX + "graphTypeDependence";
    private static final String PROP_SCHEMA_ENDPOINT_TYPE = PROP_SCHEMA_RULE_PREFIX + "endpointType";
    private static final String PROP_SCHEMA_ENDPOINT_LABEL_ID = PROP_SCHEMA_RULE_PREFIX + "endpointLabelId";
    private static final String PROP_SCHEMA_RULE_NAME = PROP_SCHEMA_RULE_PREFIX + "name";
    private static final String PROP_OWNED_INDEX = PROP_SCHEMA_RULE_PREFIX + "ownedIndex";
    public static final String PROP_OWNING_CONSTRAINT = PROP_SCHEMA_RULE_PREFIX + "owningConstraint";
    private static final String PROP_INDEX_PROVIDER_NAME = PROP_SCHEMA_RULE_PREFIX + "indexProviderName";
    private static final String PROP_INDEX_PROVIDER_VERSION = PROP_SCHEMA_RULE_PREFIX + "indexProviderVersion";
    private static final String PROP_SCHEMA_DESCRIPTOR_ENTITY_TYPE = PROP_SCHEMA_RULE_PREFIX + "schemaEntityType";
    private static final String PROP_SCHEMA_DESCRIPTOR_ENTITY_IDS = PROP_SCHEMA_RULE_PREFIX + "schemaEntityIds";
    private static final String PROP_SCHEMA_DESCRIPTOR_PROPERTY_IDS = PROP_SCHEMA_RULE_PREFIX + "schemaPropertyIds";

    // The class name PropertySchemaType has been renamed to SchemaPatternMatchingType,
    // but this key is kept as the old string for backwards compatibility
    private static final String PROP_SCHEMA_DESCRIPTOR_SCHEMA_PATTERN_MATCHING_TYPE =
            PROP_SCHEMA_RULE_PREFIX + "schemaPropertySchemaType";

    private static final String PROP_INDEX_TYPE = PROP_SCHEMA_RULE_PREFIX + "indexType";
    private static final String PROP_CONSTRAINT_ALLOWED_TYPES = PROP_SCHEMA_RULE_PREFIX + "propertyType";
    private static final String PROP_INDEX_CONFIG_PREFIX = PROP_SCHEMA_RULE_PREFIX + "IndexConfig.";

    /**
     * Turn a {@link SchemaRule} into a map-of-string-to-value representation.
     * @param rule the schema rule to convert.
     * @return a map representation of the given schema rule.
     * @see #unmapifySchemaRule(long, Map)
     */
    public static Map<String, Value> mapifySchemaRule(SchemaRule rule) {
        Map<String, Value> map = new HashMap<>();
        putStringProperty(map, PROP_SCHEMA_RULE_NAME, rule.getName());

        // Schema
        schemaDescriptorToMap(rule.schema(), map);

        // Rule
        if (rule instanceof IndexDescriptor index) {
            schemaIndexToMap(index, map);
        } else if (rule instanceof ConstraintDescriptor constraint) {
            schemaConstraintToMap(constraint, map);
        }

        return map;
    }

    /**
     * Turn a map-of-string-to-value representation of a schema rule, into an actual {@link SchemaRule} object.
     * @param ruleId the id of the rule.
     * @param map the map representation of the schema rule.
     * @return the schema rule object represented by the given map.
     * @throws MalformedSchemaRuleException if the map cannot be cleanly converted to a schema rule.
     * @see #mapifySchemaRule(SchemaRule)
     */
    public static SchemaRule unmapifySchemaRule(long ruleId, Map<String, Value> map)
            throws MalformedSchemaRuleException {
        String schemaRuleType = getString(PROP_SCHEMA_RULE_TYPE, map);
        return switch (schemaRuleType) {
            case "INDEX" -> buildIndexRule(ruleId, map);
            case "CONSTRAINT" -> buildConstraintRule(ruleId, map);
            default -> throw new MalformedSchemaRuleException(
                    "Can not create a schema rule of type: " + schemaRuleType);
        };
    }

    private static void schemaDescriptorToMap(SchemaDescriptor schemaDescriptor, Map<String, Value> map) {
        EntityType entityType = schemaDescriptor.entityType();
        SchemaPatternMatchingType schemaPatternMatchingType = schemaDescriptor.schemaPatternMatchingType();
        int[] entityTokenIds = schemaDescriptor.getEntityTokenIds();
        int[] propertyIds = schemaDescriptor.getPropertyIds();

        putStringProperty(map, PROP_SCHEMA_DESCRIPTOR_ENTITY_TYPE, entityType.name());
        putStringProperty(map, PROP_SCHEMA_DESCRIPTOR_SCHEMA_PATTERN_MATCHING_TYPE, schemaPatternMatchingType.name());
        putIntArrayProperty(map, PROP_SCHEMA_DESCRIPTOR_ENTITY_IDS, entityTokenIds);
        putIntArrayProperty(map, PROP_SCHEMA_DESCRIPTOR_PROPERTY_IDS, propertyIds);
    }

    private static void indexConfigToMap(IndexConfig indexConfig, Map<String, Value> map) {
        RichIterable<Pair<String, Value>> entries = indexConfig.entries();
        for (Pair<String, Value> entry : entries) {
            putIndexConfigProperty(map, entry.getOne(), entry.getTwo());
        }
    }

    private static void schemaIndexToMap(IndexDescriptor rule, Map<String, Value> map) {
        // Rule
        putStringProperty(map, PROP_SCHEMA_RULE_TYPE, "INDEX");

        IndexType indexType = rule.getIndexType();
        putStringProperty(map, PROP_INDEX_TYPE, indexType.name());

        if (rule.isUnique()) {
            putStringProperty(map, PROP_INDEX_RULE_TYPE, "UNIQUE");
            if (rule.getOwningConstraintId().isPresent()) {
                map.put(
                        PROP_OWNING_CONSTRAINT,
                        Values.longValue(rule.getOwningConstraintId().getAsLong()));
            }
        } else {
            putStringProperty(map, PROP_INDEX_RULE_TYPE, "NON_UNIQUE");
        }

        // Provider
        indexProviderToMap(rule, map);

        // Index config
        IndexConfig indexConfig = rule.getIndexConfig();
        indexConfigToMap(indexConfig, map);
    }

    private static void indexProviderToMap(IndexDescriptor rule, Map<String, Value> map) {
        IndexProviderDescriptor provider = rule.getIndexProvider();
        String name = provider.getKey();
        String version = provider.getVersion();
        putStringProperty(map, PROP_INDEX_PROVIDER_NAME, name);
        putStringProperty(map, PROP_INDEX_PROVIDER_VERSION, version);
    }

    private static void schemaConstraintToMap(ConstraintDescriptor rule, Map<String, Value> map) {
        // Rule
        ConstraintType type = rule.type();
        putStringProperty(map, PROP_SCHEMA_RULE_TYPE, "CONSTRAINT");
        putStringProperty(map, PROP_CONSTRAINT_RULE_TYPE, type.name());
        putStringProperty(
                map,
                PROP_SCHEMA_GRAPH_TYPE_DEPENDENCE,
                rule.graphTypeDependence().name());
        switch (type) {
            case UNIQUE, UNIQUE_EXISTS -> {
                IndexBackedConstraintDescriptor indexBackedConstraint = rule.asIndexBackedConstraint();
                putStringProperty(
                        map, PROP_INDEX_TYPE, indexBackedConstraint.indexType().name());
                if (indexBackedConstraint.hasOwnedIndexId()) {
                    putLongProperty(map, PROP_OWNED_INDEX, indexBackedConstraint.ownedIndexId());
                }
            }
            case PROPERTY_TYPE -> {
                TypeConstraintDescriptor typeConstraintDescriptor = rule.asPropertyTypeConstraint();
                PropertyTypeSet schemaValueTypes = typeConstraintDescriptor.propertyType();
                String[] typeArray = new String[schemaValueTypes.size()];
                int i = 0;
                for (SchemaValueType schemaValueType : schemaValueTypes) {
                    typeArray[i++] = schemaValueType.serialize();
                }
                putStringArrayProperty(map, PROP_CONSTRAINT_ALLOWED_TYPES, typeArray);
            }
            case ENDPOINT -> {
                RelationshipEndpointConstraintDescriptor endpointConstraintDescriptor =
                        rule.asRelationshipEndpointConstraint();
                putStringProperty(
                        map,
                        PROP_SCHEMA_ENDPOINT_TYPE,
                        endpointConstraintDescriptor.endpointType().name());
                putLongProperty(map, PROP_SCHEMA_ENDPOINT_LABEL_ID, endpointConstraintDescriptor.endpointLabelId());
            }

            default -> {}
        }
    }

    private static int[] getIntArray(String property, Map<String, Value> props) throws MalformedSchemaRuleException {
        Value value = props.get(property);
        if (value instanceof IntArray intArray) {
            return intArray.asObject();
        }
        throw new MalformedSchemaRuleException("Expected property " + property + " to be a IntArray but was " + value);
    }

    private static long getLong(String property, Map<String, Value> props) throws MalformedSchemaRuleException {
        Value value = props.get(property);
        if (value instanceof LongValue longValue) {
            return longValue.value();
        }
        throw new MalformedSchemaRuleException("Expected property " + property + " to be a LongValue but was " + value);
    }

    private static OptionalLong getOptionalLong(String property, Map<String, Value> props) {
        Value value = props.get(property);
        if (value instanceof LongValue longValue) {
            return OptionalLong.of(longValue.value());
        }
        return OptionalLong.empty();
    }

    private static Optional<String> getOptionalString(String property, Map<String, Value> map) {
        Value value = map.get(property);
        if (value instanceof TextValue textValue) {
            return Optional.of(textValue.stringValue());
        }
        return Optional.empty();
    }

    private static String getString(String property, Map<String, Value> map) throws MalformedSchemaRuleException {
        Value value = map.get(property);
        if (value instanceof TextValue textValue) {
            return textValue.stringValue();
        }
        throw new MalformedSchemaRuleException("Expected property " + property + " to be a TextValue but was " + value);
    }

    private static String[] getStringArray(String property, Map<String, Value> props)
            throws MalformedSchemaRuleException {
        Value value = props.get(property);
        if (value instanceof StringArray stringArray) {
            return stringArray.asObject();
        }
        throw new MalformedSchemaRuleException(
                "Expected property " + property + " to be a StringArray but was " + value);
    }

    private static void putLongProperty(Map<String, Value> map, String property, long value) {
        map.put(property, Values.longValue(value));
    }

    private static void putIntArrayProperty(Map<String, Value> map, String property, int[] value) {
        map.put(property, Values.intArray(value));
    }

    private static void putStringProperty(Map<String, Value> map, String property, String value) {
        map.put(property, Values.stringValue(value));
    }

    private static void putStringArrayProperty(Map<String, Value> map, String property, String[] value) {
        map.put(property, Values.stringArray(value));
    }

    private static void putIndexConfigProperty(Map<String, Value> map, String key, Value value) {
        map.put(PROP_INDEX_CONFIG_PREFIX + key, value);
    }

    private static SchemaRule buildIndexRule(long schemaRuleId, Map<String, Value> props)
            throws MalformedSchemaRuleException {
        SchemaDescriptor schema = buildSchemaDescriptor(props);
        String indexRuleType = getString(PROP_INDEX_RULE_TYPE, props);
        boolean unique = parseIndexType(indexRuleType);

        IndexPrototype prototype = unique ? IndexPrototype.uniqueForSchema(schema) : IndexPrototype.forSchema(schema);

        prototype = prototype.withName(getString(PROP_SCHEMA_RULE_NAME, props));
        prototype = prototype.withIndexType(getIndexType(getString(PROP_INDEX_TYPE, props)));

        String providerKey = getString(PROP_INDEX_PROVIDER_NAME, props);
        String providerVersion = getString(PROP_INDEX_PROVIDER_VERSION, props);
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor(providerKey, providerVersion);
        prototype = prototype.withIndexProvider(providerDescriptor);

        IndexDescriptor index = prototype.materialise(schemaRuleId);

        IndexConfig indexConfig = extractIndexConfig(props);
        index = index.withIndexConfig(indexConfig);

        if (props.containsKey(PROP_OWNING_CONSTRAINT)) {
            index = index.withOwningConstraintId(getLong(PROP_OWNING_CONSTRAINT, props));
        }

        return index;
    }

    private static boolean parseIndexType(String indexRuleType) throws MalformedSchemaRuleException {
        return switch (indexRuleType) {
            case "NON_UNIQUE" -> false;
            case "UNIQUE" -> true;
            default -> throw new MalformedSchemaRuleException("Did not recognize index rule type: " + indexRuleType);
        };
    }

    private static SchemaRule buildConstraintRule(long id, Map<String, Value> props)
            throws MalformedSchemaRuleException {
        SchemaDescriptor schema = buildSchemaDescriptor(props);
        ConstraintType constraintRuleType = getConstraintType(getString(PROP_CONSTRAINT_RULE_TYPE, props));

        GraphTypeDependence graphTypeDependence =
                getGraphTypeDependence(constraintRuleType, getOptionalString(PROP_SCHEMA_GRAPH_TYPE_DEPENDENCE, props));

        String name = getString(PROP_SCHEMA_RULE_NAME, props);
        OptionalLong ownedIndex = getOptionalLong(PROP_OWNED_INDEX, props);
        ConstraintDescriptor constraint =
                getConstraintDescriptor(constraintRuleType, graphTypeDependence, schema, ownedIndex, props);
        return constraint.withId(id).withName(name);
    }

    private static ConstraintDescriptor getConstraintDescriptor(
            ConstraintType constraintRuleType,
            GraphTypeDependence graphTypeDependence,
            SchemaDescriptor schema,
            OptionalLong ownedIndex,
            Map<String, Value> props)
            throws MalformedSchemaRuleException {
        return switch (constraintRuleType) {
            case UNIQUE -> {
                var constraint = ConstraintDescriptorFactory.uniqueForSchema(
                        schema, getIndexType(getString(PROP_INDEX_TYPE, props)));

                if (ownedIndex.isPresent()) {
                    constraint = constraint.withOwnedIndexId(ownedIndex.getAsLong());
                }
                yield constraint;
            }

            case EXISTS -> ConstraintDescriptorFactory.existsForSchema(
                    schema, graphTypeDependence == GraphTypeDependence.DEPENDENT);

            case UNIQUE_EXISTS -> {
                var constraint = ConstraintDescriptorFactory.keyForSchema(
                        schema, getIndexType(getString(PROP_INDEX_TYPE, props)));

                if (ownedIndex.isPresent()) {
                    constraint = constraint.withOwnedIndexId(ownedIndex.getAsLong());
                }
                yield constraint;
            }

            case PROPERTY_TYPE -> ConstraintDescriptorFactory.typeForSchema(
                    schema,
                    getAllowedTypes(getStringArray(PROP_CONSTRAINT_ALLOWED_TYPES, props)),
                    graphTypeDependence == GraphTypeDependence.DEPENDENT);

            case ENDPOINT -> ConstraintDescriptorFactory.relationshipEndpointForSchema(
                    schema.asSchemaDescriptorType(RelationshipEndpointSchemaDescriptor.class),
                    (int) getLong(PROP_SCHEMA_ENDPOINT_LABEL_ID, props),
                    getEndpointType(props));
        };
    }

    private static SchemaDescriptor buildSchemaDescriptor(Map<String, Value> props)
            throws MalformedSchemaRuleException {
        EntityType entityType = getEntityType(getString(PROP_SCHEMA_DESCRIPTOR_ENTITY_TYPE, props));
        SchemaPatternMatchingType schemaPatternMatchingType =
                getSchemaPatternMatchingType(getString(PROP_SCHEMA_DESCRIPTOR_SCHEMA_PATTERN_MATCHING_TYPE, props));
        int[] entityIds = getIntArray(PROP_SCHEMA_DESCRIPTOR_ENTITY_IDS, props);
        int[] propertyIds = getIntArray(PROP_SCHEMA_DESCRIPTOR_PROPERTY_IDS, props);

        return new SchemaDescriptorImplementation(entityType, schemaPatternMatchingType, entityIds, propertyIds);
    }

    private static IndexConfig extractIndexConfig(Map<String, Value> props) {
        Map<String, Value> configMap = new HashMap<>();
        for (Map.Entry<String, Value> entry : props.entrySet()) {
            if (entry.getKey().startsWith(PROP_INDEX_CONFIG_PREFIX)) {
                configMap.put(entry.getKey().substring(PROP_INDEX_CONFIG_PREFIX.length()), entry.getValue());
            }
        }
        return IndexConfig.with(configMap);
    }

    private static IndexType getIndexType(String indexType) throws MalformedSchemaRuleException {
        try {
            return IndexType.valueOf(indexType);
        } catch (Exception e) {
            throw new MalformedSchemaRuleException("Did not recognize index type: " + indexType, e);
        }
    }

    private static SchemaPatternMatchingType getSchemaPatternMatchingType(String schemaPatternMatchingType)
            throws MalformedSchemaRuleException {
        try {
            return SchemaPatternMatchingType.valueOf(schemaPatternMatchingType);
        } catch (Exception e) {
            throw new MalformedSchemaRuleException(
                    "Did not recognize schema pattern matching type: " + schemaPatternMatchingType, e);
        }
    }

    private static EntityType getEntityType(String entityType) throws MalformedSchemaRuleException {
        try {
            return EntityType.valueOf(entityType);
        } catch (Exception e) {
            throw new MalformedSchemaRuleException("Did not recognize entity type: " + entityType, e);
        }
    }

    private static PropertyTypeSet getAllowedTypes(String[] allowedTypes) throws MalformedSchemaRuleException {
        List<SchemaValueType> types = new ArrayList<>();
        for (String allowedType : allowedTypes) {
            try {
                types.add(SchemaValueTypes.convertToSchemaValueType(allowedType));
            } catch (Exception e) {
                throw new MalformedSchemaRuleException(
                        "Did not recognize schema value type '%s' in: %s"
                                .formatted(allowedType, Arrays.toString(allowedTypes)),
                        e);
            }
        }
        return PropertyTypeSet.of(types);
    }

    static GraphTypeDependence getGraphTypeDependence(
            ConstraintType constraintType, Optional<String> maybeGraphTypeDependence)
            throws MalformedSchemaRuleException {
        if (maybeGraphTypeDependence.isPresent()) {
            GraphTypeDependence graphTypeDependence;
            try {
                graphTypeDependence = GraphTypeDependence.valueOf(maybeGraphTypeDependence.get());
            } catch (Exception e) {
                throw new MalformedSchemaRuleException(
                        "Did not recognize constraint dependency type: " + maybeGraphTypeDependence.get(), e);
            }
            if (constraintType.enforcesUniqueness() && graphTypeDependence != GraphTypeDependence.UNDESIGNATED) {
                throw new MalformedSchemaRuleException("incompatible graph type dependence " + graphTypeDependence
                        + " with constraint rule type " + constraintType);
            }
            return graphTypeDependence;
        }
        return constraintType.enforcesUniqueness() ? GraphTypeDependence.UNDESIGNATED : GraphTypeDependence.INDEPENDENT;
    }

    static EndpointType getEndpointType(Map<String, Value> props) throws MalformedSchemaRuleException {
        Optional<String> maybeEndpointType = getOptionalString(PROP_SCHEMA_ENDPOINT_TYPE, props);
        if (maybeEndpointType.isPresent()) {
            String enumName = maybeEndpointType.get();
            try {
                return EndpointType.valueOf(enumName);
            } catch (IllegalArgumentException e) {
                throw new MalformedSchemaRuleException("Endpoint type with name " + enumName + " not recognized");
            }
        } else {
            throw new MalformedSchemaRuleException("Endpoint type of endpoint label constraint not found");
        }
    }

    static ConstraintType getConstraintType(String constraintType) throws MalformedSchemaRuleException {
        try {
            return ConstraintType.valueOf(constraintType);
        } catch (Exception e) {
            throw new MalformedSchemaRuleException("Did not recognize constraint rule type: " + constraintType);
        }
    }
}
