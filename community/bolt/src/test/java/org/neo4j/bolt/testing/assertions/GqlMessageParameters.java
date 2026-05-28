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
package org.neo4j.bolt.testing.assertions;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.gqlstatus.GqlParams.GqlParam;

public final class GqlMessageParameters {
    private static final GqlMessageParameters EMPTY = new GqlMessageParameters();

    private final List<Object> parameters;

    private GqlMessageParameters() {
        this(List.of());
    }

    private GqlMessageParameters(List<Object> parameters) {
        this.parameters = parameters;
    }

    public static GqlMessageParameters create() {
        return EMPTY;
    }

    public Object[] toArray() {
        return this.parameters.toArray();
    }

    protected GqlMessageParameters withParam(Object param) {
        var copy = new ArrayList<>(this.parameters);
        copy.add(param);
        return new GqlMessageParameters(copy);
    }

    public GqlMessageParameters withParam(GqlParam param) {
        return this.withParam(param.toParamFormat());
    }

    public GqlMessageParameters withByte(byte value) {
        return this.withParam(value);
    }

    public GqlMessageParameters withShort(short value) {
        return this.withParam(value);
    }

    public GqlMessageParameters withInt(int value) {
        return this.withParam(value);
    }

    public GqlMessageParameters withLong(long value) {
        return this.withParam(value);
    }

    public GqlMessageParameters withString(String value) {
        return this.withParam(value);
    }

    public GqlMessageParameters withList(Object... values) {
        return this.withParam(List.of(values));
    }
}
