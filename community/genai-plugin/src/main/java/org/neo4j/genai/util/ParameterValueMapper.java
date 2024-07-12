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
package org.neo4j.genai.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

public class ParameterValueMapper implements ValueMapper<Object> {
    private Object unsupported(String thing) {
        throw new IllegalArgumentException("%s are not supported as parameter values".formatted(thing));
    }

    @Override
    public Object mapPath(VirtualPathValue value) {
        return unsupported("paths");
    }

    @Override
    public Object mapNode(VirtualNodeValue value) {
        return unsupported("nodes");
    }

    @Override
    public Object mapRelationship(VirtualRelationshipValue value) {
        return unsupported("relationships");
    }

    @Override
    public Object mapNoValue() {
        return null;
    }

    @Override
    public Object mapMap(MapValue value) {
        final var map = new HashMap<String, Object>(value.size());
        value.foreach((k, v) -> map.put(k, v.map(this)));
        return map;
    }

    @Override
    public List<?> mapSequence(SequenceValue value) {
        final var size = value.length();
        final var list = new ArrayList<>(size);
        if (value.iterationPreference() == SequenceValue.IterationPreference.RANDOM_ACCESS) {
            for (int i = 0; i < size; i++) {
                list.add(value.value(i).map(this));
            }
        } else {
            value.forEach(v -> list.add(v.map(this)));
        }
        return list;
    }

    @Override
    public Object mapText(TextValue value) {
        return value.stringValue();
    }

    @Override
    public Object mapBoolean(BooleanValue value) {
        return value.booleanValue();
    }

    @Override
    public Object mapNumber(NumberValue value) {
        return unsupported("numbers");
    }

    @Override
    public Object mapIntegral(IntegralValue value) {
        return value.longValue();
    }

    @Override
    public Object mapFloatingPoint(FloatingPointValue value) {
        return value.doubleValue();
    }

    @Override
    public Object mapDateTime(DateTimeValue value) {
        return value.asObjectCopy();
    }

    @Override
    public Object mapLocalDateTime(LocalDateTimeValue value) {
        return value.asObjectCopy();
    }

    @Override
    public Object mapDate(DateValue value) {
        return value.asObjectCopy();
    }

    @Override
    public Object mapTime(TimeValue value) {
        return value.asObjectCopy();
    }

    @Override
    public Object mapLocalTime(LocalTimeValue value) {
        return value.asObjectCopy();
    }

    @Override
    public Object mapDuration(DurationValue value) {
        return value.asObjectCopy();
    }

    @Override
    public Object mapPoint(PointValue value) {
        return value.asObjectCopy();
    }
}
