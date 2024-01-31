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

import static org.neo4j.exceptions.InvalidSpatialArgumentException.invalidCoordinateSystem;

import java.util.Locale;
import java.util.Objects;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.internal.helpers.collection.Iterables;

public enum CoordinateReferenceSystem implements CRS {
    CARTESIAN(CRSTable.SR_ORG, 7203, 2, false),
    CARTESIAN_3D(CRSTable.SR_ORG, 9157, 3, false),
    WGS_84(CRSTable.EPSG, 4326, 2, true),
    WGS_84_3D(CRSTable.EPSG, 4979, 3, true);

    private static final CoordinateReferenceSystem[] COORDINATE_REFERENCE_SYSTEMS = values();

    public static Iterable<CoordinateReferenceSystem> all() {
        return Iterables.asIterable(COORDINATE_REFERENCE_SYSTEMS);
    }

    public static CoordinateReferenceSystem get(int tableId, int code) {
        final var table = CRSTable.find(tableId);
        for (final var crs : COORDINATE_REFERENCE_SYSTEMS) {
            if (crs.table == table && crs.code == code) {
                return crs;
            }
        }
        return invalidCoordinateSystem(tableId + "-" + code);
    }

    public static CoordinateReferenceSystem get(CRS crs) {
        Objects.requireNonNull(crs);
        return get(crs.getHref());
    }

    public static CoordinateReferenceSystem byName(String name) {
        for (final var crs : COORDINATE_REFERENCE_SYSTEMS) {
            if (crs.name.equals(name.toLowerCase(Locale.ROOT))) {
                return crs;
            }
        }
        return invalidCoordinateSystem(name);
    }

    public static CoordinateReferenceSystem get(String href) {
        for (final var crs : COORDINATE_REFERENCE_SYSTEMS) {
            if (crs.href.equals(href)) {
                return crs;
            }
        }
        return invalidCoordinateSystem(href);
    }

    public static CoordinateReferenceSystem get(int code) {
        for (final var table : CRSTable.TYPES) {
            final var href = table.href(code);
            for (final var crs : COORDINATE_REFERENCE_SYSTEMS) {
                if (crs.href.equals(href)) {
                    return crs;
                }
            }
        }
        return invalidCoordinateSystem(code);
    }

    private final String name;
    private final CRSTable table;
    private final int code;
    private final String href;
    private final int dimension;
    private final boolean geographic;
    private final CRSCalculator calculator;

    CoordinateReferenceSystem(CRSTable table, int code, int dimension, boolean geographic) {
        this.name = name().toLowerCase(Locale.ROOT).replace('_', '-');
        this.table = table;
        this.code = code;
        this.href = table.href(code);
        this.dimension = dimension;
        this.geographic = geographic;
        if (geographic) {
            this.calculator = new CRSCalculator.GeographicCalculator(dimension);
        } else {
            this.calculator = new CRSCalculator.CartesianCalculator(dimension);
        }
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int getCode() {
        return code;
    }

    @Override
    public String getType() {
        return name;
    }

    @Override
    public String getHref() {
        return href;
    }

    public String getName() {
        return name;
    }

    public CRSTable getTable() {
        return table;
    }

    public int getDimension() {
        return dimension;
    }

    public boolean isGeographic() {
        return geographic;
    }

    public CRSCalculator getCalculator() {
        return calculator;
    }
}
