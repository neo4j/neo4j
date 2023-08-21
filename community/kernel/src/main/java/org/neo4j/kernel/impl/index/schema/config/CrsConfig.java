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
package org.neo4j.kernel.impl.index.schema.config;

import static org.neo4j.configuration.SettingConstraints.size;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.listOf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.configuration.GroupSettingHelper;
import org.neo4j.configuration.SettingBuilder;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.values.storable.CoordinateReferenceSystem;

@ServiceProvider
public class CrsConfig implements GroupSetting {
    private static final String PREFIX = "internal.dbms.db.spatial.crs";

    public final Setting<List<Double>> min;
    public final Setting<List<Double>> max;
    public final CoordinateReferenceSystem crs;
    private final String name;

    public static CrsConfig group(CoordinateReferenceSystem crs) {
        return new CrsConfig(crs.getName());
    }

    public CrsConfig() {
        this(null);
    }

    private CrsConfig(String name) {
        this.name = name;
        if (name != null) {
            crs = CoordinateReferenceSystem.byName(name);
            List<Double> defaultValue = new ArrayList<>(Collections.nCopies(crs.getDimension(), Double.NaN));
            min = getBuilder("min", listOf(DOUBLE), defaultValue)
                    .internal()
                    .addConstraint(size(crs.getDimension()))
                    .build();
            max = getBuilder("max", listOf(DOUBLE), defaultValue)
                    .internal()
                    .addConstraint(size(crs.getDimension()))
                    .build();
        } else {
            crs = null;
            min = null;
            max = null;
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    private <T> SettingBuilder<T> getBuilder(String suffix, SettingValueParser<T> parser, T defaultValue) {
        return GroupSettingHelper.getBuilder(getPrefix(), name(), suffix, parser, defaultValue);
    }
}
