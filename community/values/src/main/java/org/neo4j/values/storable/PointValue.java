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
package org.neo4j.values.storable;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.neo4j.exceptions.InvalidSpatialArgumentException.infiniteCoordinateValue;
import static org.neo4j.exceptions.InvalidSpatialArgumentException.invalidDimension;
import static org.neo4j.exceptions.InvalidSpatialArgumentException.invalidGeographicCoordinates;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOf;
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.Comparison;
import org.neo4j.values.Equality;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.utils.PrettyPrinter;
import org.neo4j.values.virtual.MapValue;

public class PointValue extends HashMemoizingScalarValue implements Point, Comparable<PointValue> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(PointValue.class);
    static final long SIZE_2D = SHALLOW_SIZE + sizeOf(new double[2]);
    static final long SIZE_3D = SHALLOW_SIZE + sizeOf(new double[3]);

    static final PointValue MIN_VALUE_WGS_84 = new PointValue(CoordinateReferenceSystem.WGS_84, -180, -90);
    static final PointValue MAX_VALUE_WGS_84 = new PointValue(CoordinateReferenceSystem.WGS_84, 180, 90);
    static final PointValue MIN_VALUE_WGS_84_3D =
            new PointValue(CoordinateReferenceSystem.WGS_84_3D, -180, -90, -Double.MAX_VALUE);
    static final PointValue MAX_VALUE_WGS_84_3D =
            new PointValue(CoordinateReferenceSystem.WGS_84_3D, 180, 90, Double.MAX_VALUE);
    static final PointValue MIN_VALUE_CARTESIAN =
            new PointValue(CoordinateReferenceSystem.CARTESIAN, -Double.MAX_VALUE, -Double.MAX_VALUE);
    static final PointValue MAX_VALUE_CARTESIAN =
            new PointValue(CoordinateReferenceSystem.CARTESIAN, Double.MAX_VALUE, Double.MAX_VALUE);
    static final PointValue MIN_VALUE_CARTESIAN_3D = new PointValue(
            CoordinateReferenceSystem.CARTESIAN_3D, -Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE);
    static final PointValue MAX_VALUE_CARTESIAN_3D = new PointValue(
            CoordinateReferenceSystem.CARTESIAN_3D, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);

    public static PointValue minPointValueOf(CoordinateReferenceSystem crs) {
        return switch (crs) {
            case WGS_84 -> MIN_VALUE_WGS_84;
            case WGS_84_3D -> MIN_VALUE_WGS_84_3D;
            case CARTESIAN -> MIN_VALUE_CARTESIAN;
            case CARTESIAN_3D -> MIN_VALUE_CARTESIAN_3D;
        };
    }

    public static PointValue maxPointValueOf(CoordinateReferenceSystem crs) {
        return switch (crs) {
            case WGS_84 -> MAX_VALUE_WGS_84;
            case WGS_84_3D -> MAX_VALUE_WGS_84_3D;
            case CARTESIAN -> MAX_VALUE_CARTESIAN;
            case CARTESIAN_3D -> MAX_VALUE_CARTESIAN_3D;
        };
    }

    public static final PointValue MIN_VALUE = Arrays.stream(CoordinateReferenceSystem.values())
            .map(PointValue::minPointValueOf)
            .min(Values.COMPARATOR)
            .orElseThrow();

    public static final PointValue MAX_VALUE = Arrays.stream(CoordinateReferenceSystem.values())
            .map(PointValue::maxPointValueOf)
            .max(Values.COMPARATOR)
            .orElseThrow();

    private final CoordinateReferenceSystem crs;
    private final double[] coordinate;

    PointValue(CoordinateReferenceSystem crs, double... coordinate) {
        this.crs = crs;
        this.coordinate = coordinate;
        for (double c : coordinate) {
            if (!Double.isFinite(c)) {
                infiniteCoordinateValue(coordinate);
            }
        }
        if (coordinate.length != crs.getDimension()) {
            invalidDimension(crs.toString(), crs.getDimension(), coordinate);
        }
        if (crs.isGeographic() && (coordinate.length == 2 || coordinate.length == 3)) {
            // anything with less or more coordinates gets a pass as it is and needs to be stopped from other places
            // like bolt does
            //   (@see org.neo4j.bolt.v2.messaging.Neo4jPackV2Test#shouldFailToPackPointWithIllegalDimensions )
            if (coordinate[1] > 90 || coordinate[1] < -90) {
                invalidGeographicCoordinates(coordinate);
            }

            double x = coordinate[0];
            // Valid range for X is  [-180,180]
            while (x > 180) {
                x = x - 360;
            }
            while (x < -180) {
                x = x + 360;
            }
            this.coordinate[0] = x;
        }
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writePoint(getCoordinateReferenceSystem(), coordinate);
    }

    @Override
    public String prettyPrint() {
        PrettyPrinter prettyPrinter = new PrettyPrinter();
        this.writeTo(prettyPrinter);
        return prettyPrinter.value();
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.GEOMETRY;
    }

    @Override
    public NumberType numberType() {
        return NumberType.NO_NUMBER;
    }

    @Override
    public boolean equals(Value other) {
        if (other instanceof PointValue pv) {
            return Arrays.equals(this.coordinate, pv.coordinate)
                    && this.getCoordinateReferenceSystem().equals(pv.getCoordinateReferenceSystem());
        }
        return false;
    }

    public boolean equals(Point other) {
        if (!other.getCRS().getHref().equals(this.getCRS().getHref())) {
            return false;
        }
        double[] otherCoordinates = other.getCoordinate().getCoordinate();
        return Arrays.equals(coordinate, otherCoordinates);
    }

    @Override
    public boolean equalTo(Object other) {
        return other != null
                && ((other instanceof Value && equals((Value) other))
                        || (other instanceof Point && equals((Point) other)));
    }

    @Override
    public int compareTo(PointValue other) {
        int cmpCRS = Integer.compare(this.crs.getCode(), other.crs.getCode());
        if (cmpCRS != 0) {
            return cmpCRS;
        }

        // TODO: This is unnecessary and can be an assert. Is it even correct? This implies e.g. that all 2D points are
        // before all 3D regardless of x and y
        if (this.coordinate.length > other.coordinate.length) {
            return 1;
        } else if (this.coordinate.length < other.coordinate.length) {
            return -1;
        }

        for (int i = 0; i < coordinate.length; i++) {
            int cmpVal = Double.compare(this.coordinate[i], other.coordinate[i]);
            if (cmpVal != 0) {
                return cmpVal;
            }
        }
        return 0;
    }

    @Override
    protected int unsafeCompareTo(Value otherValue) {
        return compareTo((PointValue) otherValue);
    }

    @Override
    public Comparison unsafeTernaryCompareTo(Value otherValue) {
        if (ternaryEquals(otherValue) == Equality.TRUE) {
            return Comparison.EQUAL;
        } else {
            return Comparison.UNDEFINED;
        }
    }

    @Override
    public boolean isIncomparableType() {
        return true;
    }

    @Override
    public Point asObjectCopy() {
        return this;
    }

    public CoordinateReferenceSystem getCoordinateReferenceSystem() {
        return crs;
    }

    /*
     * Consumers must not modify the returned array.
     */
    public double[] coordinate() {
        return this.coordinate;
    }

    @Override
    protected int computeHashToMemoize() {
        int result = 1;
        result = HASH_CONSTANT * result + NumberValues.hash(crs.getCode());
        result = HASH_CONSTANT * result + NumberValues.hash(coordinate);
        return result;
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        hash = hashFunction.update(hash, crs.getCode());
        for (double v : coordinate) {
            hash = hashFunction.update(hash, Double.doubleToLongBits(v));
        }
        return hash;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapPoint(this);
    }

    @Override
    public String toString() {
        String coordString = coordinate.length == 2
                ? format("x: %s, y: %s", coordinate[0], coordinate[1])
                : format("x: %s, y: %s, z: %s", coordinate[0], coordinate[1], coordinate[2]);
        return format(
                "point({%s, crs: '%s'})",
                coordString, getCoordinateReferenceSystem().getName()); // TODO: Use getTypeName -> Breaking change
    }

    @Override
    public String getTypeName() {
        return "Point";
    }

    @Override
    public List<Coordinate> getCoordinates() {
        return singletonList(new Coordinate(coordinate));
    }

    @Override
    public CRS getCRS() {
        return crs;
    }

    @Override
    public long estimatedHeapUsage() {
        if (coordinate.length == 2) {
            return SIZE_2D;
        } else {
            return SIZE_3D;
        }
    }

    public static PointValue fromMap(MapValue map) {
        PointBuilder fields = new PointBuilder();
        map.foreach((key, value) -> fields.assign(key.toLowerCase(Locale.ROOT), value));
        return fromInputFields(fields);
    }

    public static PointValue parse(CharSequence text) {
        return PointValue.parse(text, null);
    }

    /**
     * Parses the given text into a PointValue. The information stated in the header is saved into the PointValue
     * unless it is overridden by the information in the text
     *
     * @param text the input text to be parsed into a PointValue
     * @param fieldsFromHeader must be a value obtained from {@link #parseHeaderInformation(CharSequence)} or null
     * @return a PointValue instance with information from the {@param fieldsFromHeader} and {@param text}
     */
    public static PointValue parse(CharSequence text, CSVHeaderInformation fieldsFromHeader) {
        PointBuilder fieldsFromData = parseHeaderInformation(text);
        if (fieldsFromHeader != null) {
            // Merge InputFields: Data fields override header fields
            if (!(fieldsFromHeader instanceof PointBuilder)) {
                throw new IllegalStateException("Wrong header information type: " + fieldsFromHeader);
            }
            fieldsFromData.mergeWithHeader((PointBuilder) fieldsFromHeader);
        }
        return fromInputFields(fieldsFromData);
    }

    public static PointBuilder parseHeaderInformation(CharSequence text) {
        return parseHeaderInformation(Value.parseStringMap(text));
    }

    public static PointBuilder parseHeaderInformation(Map<String, String> options) {
        PointBuilder fields = new PointBuilder();
        options.forEach(fields::assign);
        return fields;
    }

    private static CoordinateReferenceSystem findSpecifiedCRS(PointBuilder fields) {
        String crsValue = fields.crs;
        int sridValue = fields.srid;
        if (crsValue != null && sridValue != -1) {
            throw new InvalidArgumentException("Cannot specify both CRS and SRID");
        } else if (crsValue != null) {
            return CoordinateReferenceSystem.byName(crsValue);
        } else if (sridValue != -1) {
            return CoordinateReferenceSystem.get(sridValue);
        } else {
            return null;
        }
    }

    /**
     * This contains the logic to decide the default coordinate reference system based on the input fields
     */
    private static PointValue fromInputFields(PointBuilder fields) {
        CoordinateReferenceSystem crs = findSpecifiedCRS(fields);
        double[] coordinates;

        if (fields.x != null && fields.y != null) {
            coordinates =
                    fields.z != null ? new double[] {fields.x, fields.y, fields.z} : new double[] {fields.x, fields.y};
            if (crs == null) {
                crs = coordinates.length == 3
                        ? CoordinateReferenceSystem.CARTESIAN_3D
                        : CoordinateReferenceSystem.CARTESIAN;
            }
        } else if (fields.latitude != null && fields.longitude != null) {
            if (fields.z != null) {
                coordinates = new double[] {fields.longitude, fields.latitude, fields.z};
            } else if (fields.height != null) {
                coordinates = new double[] {fields.longitude, fields.latitude, fields.height};
            } else {
                coordinates = new double[] {fields.longitude, fields.latitude};
            }
            if (crs == null) {
                crs = coordinates.length == 3 ? CoordinateReferenceSystem.WGS_84_3D : CoordinateReferenceSystem.WGS_84;
            }
            if (!crs.isGeographic()) {
                throw new InvalidArgumentException(String.format(
                        "Geographic points does not support coordinate reference system: %s."
                                + "This is set either in the csv header or the actual data column",
                        crs));
            }
        } else {
            if (crs == null) {
                throw new InvalidArgumentException(
                        "A point must contain either 'x' and 'y' or 'latitude' and 'longitude'");
            }

            throw new InvalidArgumentException(String.format(
                    "A %s point must contain %s",
                    crs,
                    switch (crs) {
                        case CARTESIAN -> "'x' and 'y'";
                        case CARTESIAN_3D -> "'x', 'y' and 'z'";
                        case WGS_84 -> "'latitude' and 'longitude'";
                        case WGS_84_3D -> "'latitude', 'longitude' and 'height'";
                    }));
        }

        if (crs.getDimension() != coordinates.length) {
            throw new InvalidArgumentException(String.format(
                    "Cannot create point with %dD coordinate reference system and %d coordinates. "
                            + "Please consider using equivalent %dD coordinate reference system",
                    crs.getDimension(), coordinates.length, coordinates.length));
        }
        return Values.pointValue(crs, coordinates);
    }

    /**
     * For accessors from cypher.
     */
    public Value get(String fieldName) {
        return PointFields.fromName(fieldName).get(this);
    }

    DoubleValue getNthCoordinate(int n, String fieldName, boolean onlyGeographic) {
        if (onlyGeographic && !this.getCoordinateReferenceSystem().isGeographic()) {
            throw new InvalidArgumentException("Field: " + fieldName + " is not available on cartesian point: " + this);
        } else if (n >= this.coordinate().length) {
            throw new InvalidArgumentException("Field: " + fieldName + " is not available on point: " + this);
        } else {
            return Values.doubleValue(coordinate[n]);
        }
    }

    private static class PointBuilder implements CSVHeaderInformation {
        private String crs;
        private Double x;
        private Double y;
        private Double z;
        private Double longitude;
        private Double latitude;
        private Double height;
        private int srid = -1;

        @Override
        public void assign(String key, Object value) {
            switch (key.toLowerCase(Locale.ROOT)) {
                case "crs" -> {
                    checkUnassigned(crs, key);
                    assignTextValue(
                            key, value, str -> crs = QUOTES_PATTERN.matcher(str).replaceAll(""));
                }

                case "x" -> {
                    checkUnassigned(x, key);
                    assignFloatingPoint(key, value, i -> x = i);
                }

                case "y" -> {
                    checkUnassigned(y, key);
                    assignFloatingPoint(key, value, i -> y = i);
                }

                case "z" -> {
                    checkUnassigned(z, key);
                    assignFloatingPoint(key, value, i -> z = i);
                }

                case "longitude" -> {
                    checkUnassigned(longitude, key);
                    assignFloatingPoint(key, value, i -> longitude = i);
                }

                case "latitude" -> {
                    checkUnassigned(latitude, key);
                    assignFloatingPoint(key, value, i -> latitude = i);
                }

                case "height" -> {
                    checkUnassigned(height, key);
                    assignFloatingPoint(key, value, i -> height = i);
                }

                case "srid" -> {
                    if (srid != -1) {
                        throw new InvalidArgumentException(String.format("Duplicate field '%s' is not allowed.", key));
                    }
                    assignIntegral(key, value, i -> srid = i);
                }

                default -> {}
            }
        }

        void mergeWithHeader(PointBuilder header) {
            this.crs = this.crs == null ? header.crs : this.crs;
            this.x = this.x == null ? header.x : this.x;
            this.y = this.y == null ? header.y : this.y;
            this.z = this.z == null ? header.z : this.z;
            this.longitude = this.longitude == null ? header.longitude : this.longitude;
            this.latitude = this.latitude == null ? header.latitude : this.latitude;
            this.height = this.height == null ? header.height : this.height;
            this.srid = this.srid == -1 ? header.srid : this.srid;
        }

        private static void assignTextValue(String key, Object value, Consumer<String> assigner) {
            if (value instanceof String) {
                assigner.accept((String) value);
            } else if (value instanceof TextValue) {
                assigner.accept(((TextValue) value).stringValue());
            } else {
                throw new InvalidArgumentException(String.format("Cannot assign %s to field %s", value, key));
            }
        }

        private static void assignFloatingPoint(String key, Object value, Consumer<Double> assigner) {
            if (value instanceof String) {
                assigner.accept(assertConvertible(() -> Double.parseDouble((String) value)));
            } else if (value instanceof IntegralValue) {
                assigner.accept(((IntegralValue) value).doubleValue());
            } else if (value instanceof FloatingPointValue) {
                assigner.accept(((FloatingPointValue) value).doubleValue());
            } else {
                throw new InvalidArgumentException(String.format("Cannot assign %s to field %s", value, key));
            }
        }

        private static void assignIntegral(String key, Object value, Consumer<Integer> assigner) {
            if (value instanceof String) {
                assigner.accept(assertConvertible(() -> Integer.parseInt((String) value)));
            } else if (value instanceof IntegralValue) {
                assigner.accept((int) ((IntegralValue) value).longValue());
            } else {
                throw new InvalidArgumentException(String.format("Cannot assign %s to field %s", value, key));
            }
        }

        private static <T extends Number> T assertConvertible(Supplier<T> func) {
            try {
                return func.get();
            } catch (NumberFormatException e) {
                throw new InvalidArgumentException(e.getMessage(), e);
            }
        }

        private static void checkUnassigned(Object key, String fieldName) {
            if (key != null) {
                throw new InvalidArgumentException(String.format("Duplicate field '%s' is not allowed.", fieldName));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PointBuilder that = (PointBuilder) o;
            return srid == that.srid
                    && Objects.equals(crs, that.crs)
                    && Objects.equals(x, that.x)
                    && Objects.equals(y, that.y)
                    && Objects.equals(z, that.z)
                    && Objects.equals(longitude, that.longitude)
                    && Objects.equals(latitude, that.latitude)
                    && Objects.equals(height, that.height);
        }

        @Override
        public int hashCode() {
            return Objects.hash(crs, x, y, z, longitude, latitude, height, srid);
        }
    }
}
