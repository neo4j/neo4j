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
package org.neo4j.graphdb.schema;

import static java.util.Map.entry;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MAX;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_3D_MIN;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MAX;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_CARTESIAN_MIN;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MAX;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_3D_MIN;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MAX;
import static org.neo4j.graphdb.schema.IndexSettingImpl.SPATIAL_WGS84_MIN;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.eclipse.collections.impl.factory.Maps;
import org.neo4j.exceptions.InternalException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.InvalidSpatialArgumentException;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class IndexSettingUtil {
    public static final int DEFAULT_VECTOR_DIMENSIONS = 1024;

    @SuppressWarnings("unchecked")
    private static final Map<String, IndexSetting> INDEX_SETTING_REVERSE_LOOKUP =
            Map.ofEntries(Stream.of(IndexSettingImpl.values())
                    .map(s -> entry(s.getSettingName(), s))
                    .toArray(Map.Entry[]::new));

    /**
     * Convert from the internal format to the Core API format.
     * {@link IndexConfig} -> {@link Map} of {@link IndexSetting} and {@link Object}.
     */
    public static Map<IndexSetting, Object> toIndexSettingObjectMapFromIndexConfig(IndexConfig indexConfig) {
        Map<IndexSetting, Object> asMap = Maps.mutable.of();
        for (Map.Entry<String, Value> entry : indexConfig.entries()) {
            IndexSetting key = fromString(entry.getKey());
            if (key != null) {
                Object value = entry.getValue().asObjectCopy();
                asMap.put(key, value);
            }
        }
        return Collections.unmodifiableMap(asMap);
    }

    /**
     * Convert from the Core API format to internal format.
     * {@link Map} of {@link IndexSetting} and {@link Object} -> {@link IndexConfig}.
     */
    public static IndexConfig toIndexConfigFromIndexSettingObjectMap(Map<IndexSetting, Object> indexConfiguration) {
        Map<String, Value> collectingMap = new HashMap<>();
        for (Map.Entry<IndexSetting, Object> entry : indexConfiguration.entrySet()) {
            IndexSetting setting = entry.getKey();
            Value value = asIndexSettingValue(setting, entry.getValue());
            collectingMap.put(setting.getSettingName(), value);
        }
        return IndexConfig.with(collectingMap);
    }

    /**
     * Convert from the Procedure format to internal format.
     * {@link Map} of {@link String} and {@link Object} -> {@link IndexConfig}.
     */
    public static IndexConfig toIndexConfigFromStringObjectMap(Map<String, Object> configMap) {
        Map<IndexSetting, Object> collectingMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            String key = entry.getKey();
            IndexSetting indexSetting = asIndexSetting(key);
            collectingMap.put(indexSetting, entry.getValue());
        }
        return toIndexConfigFromIndexSettingObjectMap(collectingMap);
    }

    public static IndexSetting spatialMinSettingForCrs(CoordinateReferenceSystem crs) {
        return switch (crs.getName()) {
            case "cartesian" -> SPATIAL_CARTESIAN_MIN;
            case "cartesian-3d" -> SPATIAL_CARTESIAN_3D_MIN;
            case "wgs-84" -> SPATIAL_WGS84_MIN;
            case "wgs-84-3d" -> SPATIAL_WGS84_3D_MIN;
            default -> throw InvalidSpatialArgumentException.invalidCoordinateSystem(crs.getName());
        };
    }

    public static IndexSetting spatialMaxSettingForCrs(CoordinateReferenceSystem crs) {
        return switch (crs.getName()) {
            case "cartesian" -> SPATIAL_CARTESIAN_MAX;
            case "cartesian-3d" -> SPATIAL_CARTESIAN_3D_MAX;
            case "wgs-84" -> SPATIAL_WGS84_MAX;
            case "wgs-84-3d" -> SPATIAL_WGS84_3D_MAX;
            default -> throw InvalidSpatialArgumentException.invalidCoordinateSystem(crs.getName());
        };
    }

    @VisibleForTesting
    public static Map<IndexSetting, Object> defaultSettingsForTesting(IndexType type) {
        return switch (type) {
            case VECTOR ->
                Map.of(
                        IndexSetting.vector_Dimensions(),
                        DEFAULT_VECTOR_DIMENSIONS,
                        IndexSetting.vector_Similarity_Function(),
                        "COSINE");
            default -> Map.of();
        };
    }

    @VisibleForTesting
    public static IndexConfig defaultConfigForTest(IndexType type) {
        return toIndexConfigFromIndexSettingObjectMap(defaultSettingsForTesting(type));
    }

    /**
     * @param string Case sensitive setting name.
     * @return Corresponding {@link IndexSettingImpl} or null if no match.
     */
    private static IndexSetting fromString(String string) {
        return INDEX_SETTING_REVERSE_LOOKUP.get(string);
    }

    private static IndexSetting asIndexSetting(String key) {
        IndexSetting indexSetting = fromString(key);
        if (indexSetting == null) {
            throw InvalidArgumentException.invalidIndexConfig(key, List.copyOf(INDEX_SETTING_REVERSE_LOOKUP.keySet()));
        }
        return indexSetting;
    }

    @VisibleForTesting
    static Value asIndexSettingValue(IndexSetting setting, Object value) {
        Objects.requireNonNull(value, "Index setting value can not be null.");
        return parse(setting, value);
    }

    private static Value parse(IndexSetting indexSetting, Object value) {
        Class<?> type = indexSetting.getType();
        try {
            if (type == Boolean.class) {
                return parseAsBoolean(value);
            }
            if (type == double[].class) {
                return parseAsDoubleArray(value);
            }
            if (type == String.class) {
                return stringValue(value.toString());
            }
            if (type == Integer.class) {
                return parseAsInteger(value);
            }
            if (type == Double.class) {
                return parseAsDouble(value);
            }
        } catch (IndexSettingParseException e) {
            throw InvalidArgumentException.invalidType(
                    indexSetting.getSettingName(),
                    String.valueOf(value),
                    type.getSimpleName(),
                    value == null ? "null" : value.getClass().getSimpleName());
        }
        throw InternalException.internalError(
                "Should not happen.",
                "Missing parser for type " + type.getSimpleName() + ". This type is used by indexSetting "
                        + indexSetting.getSettingName());
    }

    private static IntValue parseAsInteger(Object value) throws IndexSettingParseException {
        if (value instanceof Number number) {
            return Values.intValue(number.intValue());
        }
        throw new IndexSettingParseException("Could not parse value '" + value + "' of type "
                + value.getClass().getSimpleName() + " as integer.");
    }

    private static DoubleValue parseAsDouble(Object value) throws IndexSettingParseException {
        if (value instanceof Number number) {
            return Values.doubleValue(number.doubleValue());
        }
        throw new IndexSettingParseException("Could not parse value '" + value + "' of type "
                + value.getClass().getSimpleName() + " as double.");
    }

    private static DoubleArray parseAsDoubleArray(Object value) throws IndexSettingParseException {
        // Primitive arrays
        if (value instanceof byte[] array) {
            double[] doubleArray = toDoubleArray(array);
            return doubleArray(doubleArray);
        }
        if (value instanceof short[] array) {
            double[] doubleArray = toDoubleArray(array);
            return doubleArray(doubleArray);
        }
        if (value instanceof int[] array) {
            double[] doubleArray = toDoubleArray(array);
            return doubleArray(doubleArray);
        }
        if (value instanceof long[] array) {
            double[] doubleArray = toDoubleArray(array);
            return doubleArray(doubleArray);
        }
        if (value instanceof float[] array) {
            double[] doubleArray = toDoubleArray(array);
            return doubleArray(doubleArray);
        }
        if (value instanceof double[] array) {
            return doubleArray(array);
        }

        // Non primitive arrays
        if (value instanceof Number[] array) {
            double[] doubleArray = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                doubleArray[i] = array[i].doubleValue();
            }
            return doubleArray(doubleArray);
        }

        // Collection
        if (value instanceof Collection<?> collection) {
            double[] doubleArray = new double[collection.size()];
            Iterator<?> iterator = collection.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                Object next = iterator.next();
                if (next instanceof Number) {
                    doubleArray[i] = ((Number) next).doubleValue();
                } else {
                    throw new IndexSettingParseException("Could not parse value '" + value + "' of type "
                            + next.getClass().getSimpleName() + " as double.");
                }
            }
            return doubleArray(doubleArray);
        }

        throw new IndexSettingParseException("Could not parse value '" + value + "' as double[].");
    }

    private static BooleanValue parseAsBoolean(Object value) throws IndexSettingParseException {
        if (value instanceof Boolean bool) {
            return booleanValue(bool);
        }
        throw new IndexSettingParseException("Could not parse value '" + value + "' as boolean.");
    }

    private static double[] toDoubleArray(byte[] value) {
        double[] doubleArray = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray(short[] value) {
        double[] doubleArray = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray(int[] value) {
        double[] doubleArray = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray(long[] value) {
        double[] doubleArray = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray(float[] value) {
        double[] doubleArray = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static class IndexSettingParseException extends Exception {
        IndexSettingParseException(String message) {
            super(message);
        }
    }
}
