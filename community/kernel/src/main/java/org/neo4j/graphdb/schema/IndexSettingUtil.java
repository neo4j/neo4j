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
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.Maps;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class IndexSettingUtil {
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
        for (Pair<String, Value> entry : indexConfig.entries()) {
            IndexSetting key = fromString(entry.getOne());
            if (key != null) {
                Object value = entry.getTwo().asObjectCopy();
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
            final Value value = asIndexSettingValue(setting, entry.getValue());
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
            final String key = entry.getKey();
            final IndexSetting indexSetting = asIndexSetting(key);
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
            default -> throw new IllegalArgumentException("Unrecognized coordinate reference system " + crs);
        };
    }

    public static IndexSetting spatialMaxSettingForCrs(CoordinateReferenceSystem crs) {
        return switch (crs.getName()) {
            case "cartesian" -> SPATIAL_CARTESIAN_MAX;
            case "cartesian-3d" -> SPATIAL_CARTESIAN_3D_MAX;
            case "wgs-84" -> SPATIAL_WGS84_MAX;
            case "wgs-84-3d" -> SPATIAL_WGS84_3D_MAX;
            default -> throw new IllegalArgumentException("Unrecognized coordinate reference system " + crs);
        };
    }

    @VisibleForTesting
    public static Map<IndexSetting, Object> defaultSettingsForTesting(IndexType type) {
        return switch (type) {
            case VECTOR -> Map.of(
                    IndexSetting.vector_Dimensions(), 1024, IndexSetting.vector_Similarity_Function(), "COSINE");
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
        final IndexSetting indexSetting = fromString(key);
        if (indexSetting == null) {
            throw new IllegalArgumentException(
                    String.format("Invalid index config key '%s', it was not recognized as an index setting.", key));
        }
        return indexSetting;
    }

    @VisibleForTesting
    static Value asIndexSettingValue(IndexSetting setting, Object value) {
        Objects.requireNonNull(value, "Index setting value can not be null.");
        return parse(setting, value);
    }

    private static Value parse(IndexSetting indexSetting, Object value) {
        final Class<?> type = indexSetting.getType();
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
        } catch (IndexSettingParseException e) {
            throw new IllegalArgumentException(
                    "Invalid value type for '" + indexSetting.getSettingName() + "' setting. "
                            + "Expected a value of type "
                            + type.getName() + ", " + "but got value '"
                            + value + "' of type "
                            + (value == null ? "null" : value.getClass().getName()) + ".",
                    e);
        }
        throw new UnsupportedOperationException("Should not happen. Missing parser for type " + type.getSimpleName()
                + ". This type is used by indexSetting " + indexSetting.getSettingName());
    }

    private static IntValue parseAsInteger(Object value) throws IndexSettingParseException {
        if (value instanceof Number) {
            return Values.intValue(((Number) value).intValue());
        }
        throw new IndexSettingParseException("Could not parse value '" + value + "' of type "
                + value.getClass().getSimpleName() + " as integer.");
    }

    private static DoubleArray parseAsDoubleArray(Object value) throws IndexSettingParseException {
        // Primitive arrays
        if (value instanceof byte[]) {
            final double[] doubleArray = toDoubleArray((byte[]) value);
            return doubleArray(doubleArray);
        }
        if (value instanceof short[]) {
            final double[] doubleArray = toDoubleArray((short[]) value);
            return doubleArray(doubleArray);
        }
        if (value instanceof int[]) {
            final double[] doubleArray = toDoubleArray((int[]) value);
            return doubleArray(doubleArray);
        }
        if (value instanceof long[]) {
            final double[] doubleArray = toDoubleArray((long[]) value);
            return doubleArray(doubleArray);
        }
        if (value instanceof float[]) {
            final double[] doubleArray = toDoubleArray((float[]) value);
            return doubleArray(doubleArray);
        }
        if (value instanceof double[]) {
            return doubleArray((double[]) value);
        }

        // Non primitive arrays
        if (value instanceof final Number[] numberArray) {
            final double[] doubleArray = new double[numberArray.length];
            for (int i = 0; i < numberArray.length; i++) {
                doubleArray[i] = numberArray[i].doubleValue();
            }
            return doubleArray(doubleArray);
        }

        // Collection
        if (value instanceof final Collection collection) {
            final double[] doubleArray = new double[collection.size()];
            final Iterator iterator = collection.iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                final Object next = iterator.next();
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
        if (value instanceof Boolean) {
            return booleanValue((Boolean) value);
        }
        throw new IndexSettingParseException("Could not parse value '" + value + "' as boolean.");
    }

    private static double[] toDoubleArray(byte[] value) {
        final double[] doubleArray = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray(short[] value) {
        final double[] doubleArray = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray(int[] value) {
        final double[] doubleArray = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray(long[] value) {
        final double[] doubleArray = new double[value.length];
        for (int i = 0; i < value.length; i++) {
            doubleArray[i] = value[i];
        }
        return doubleArray;
    }

    private static double[] toDoubleArray(float[] value) {
        final double[] doubleArray = new double[value.length];
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
