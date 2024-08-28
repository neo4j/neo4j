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
package org.neo4j.storageengine.util;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;

import java.util.Arrays;
import java.util.Objects;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.RelationshipDirection;

public class EagerDegrees implements Degrees, Degrees.Mutator {
    private static final int FIRST_TYPE_UNDECIDED = -1;

    private int firstType = FIRST_TYPE_UNDECIDED;
    private Degree firstTypeDegrees;
    private MutableIntObjectMap<Degree> degrees;

    @Override
    public boolean add(int type, int outgoing, int incoming, int loop) {
        getOrCreateDegree(type).add(outgoing, incoming, loop);
        return true;
    }

    public void add(int type, RelationshipDirection direction, int count) {
        switch (direction) {
            case OUTGOING -> addOutgoing(type, count);
            case INCOMING -> addIncoming(type, count);
            case LOOP -> addLoop(type, count);
        }
    }

    public void addOutgoing(int type, int count) {
        getOrCreateDegree(type).outgoing += count;
    }

    public void addIncoming(int type, int count) {
        getOrCreateDegree(type).incoming += count;
    }

    public void addLoop(int type, int count) {
        getOrCreateDegree(type).loop += count;
    }

    public int rawOutgoingDegree(int type) {
        Degree degree = findDegree(type);
        return degree != null ? degree.outgoing : 0;
    }

    public int rawIncomingDegree(int type) {
        Degree degree = findDegree(type);
        return degree != null ? degree.incoming : 0;
    }

    public int rawLoopDegree(int type) {
        Degree degree = findDegree(type);
        return degree != null ? degree.loop : 0;
    }

    private Degree getOrCreateDegree(int type) {
        if (firstType == FIRST_TYPE_UNDECIDED) {
            firstType = type;
            firstTypeDegrees = new Degree();
            return firstTypeDegrees;
        } else if (firstType == type) {
            return firstTypeDegrees;
        }

        if (degrees == null) {
            degrees = IntObjectMaps.mutable.empty();
        }
        return degrees.getIfAbsentPut(type, Degree::new);
    }

    @Override
    public int[] types() {
        if (firstType == FIRST_TYPE_UNDECIDED) {
            return EMPTY_INT_ARRAY;
        }
        if (degrees == null) {
            return new int[] {firstType};
        }
        int[] types = new int[degrees.size() + 1];
        types[0] = firstType;
        System.arraycopy(degrees.keySet().toArray(), 0, types, 1, degrees.size());
        return types;
    }

    @Override
    public int degree(int type, Direction direction) {
        Degree degree = findDegree(type);
        if (degree == null) {
            return 0;
        }
        return switch (direction) {
            case OUTGOING -> degree.outgoing + degree.loop;
            case INCOMING -> degree.incoming + degree.loop;
            case BOTH -> degree.outgoing + degree.incoming + degree.loop;
        };
    }

    public Degree findDegree(int type) {
        Degree degree = null;
        if (firstType == type) {
            degree = firstTypeDegrees;
        } else if (degrees != null) {
            degree = degrees.get(type);
        }
        return degree;
    }

    public void addAll(EagerDegrees degrees) {
        for (int type : degrees.types()) {
            add(type, degrees.rawOutgoingDegree(type), degrees.rawIncomingDegree(type), degrees.rawLoopDegree(type));
        }
    }

    public void clear() {
        firstType = FIRST_TYPE_UNDECIDED;
        firstTypeDegrees = null;
        degrees = null;
    }

    public boolean isEmpty() {
        if (firstTypeDegrees == null) {
            return true;
        }
        for (int type : types()) {
            if (!findDegree(type).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    public boolean hasType(int type) {
        return firstType == type || (degrees != null && degrees.containsKey(type));
    }

    @Override
    public boolean isSplit() {
        return true;
    }

    public static class Degree {
        private int outgoing;
        private int incoming;
        private int loop;

        void add(int outgoing, int incoming, int loop) {
            this.outgoing += outgoing;
            this.incoming += incoming;
            this.loop += loop;
        }

        public int outgoing() {
            return outgoing;
        }

        public int incoming() {
            return incoming;
        }

        public int loop() {
            return loop;
        }

        public int total() {
            return outgoing + incoming + loop;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Degree degree = (Degree) o;
            return outgoing == degree.outgoing && incoming == degree.incoming && loop == degree.loop;
        }

        @Override
        public int hashCode() {
            return Objects.hash(outgoing, incoming, loop);
        }

        @Override
        public String toString() {
            return "[" + "out:" + outgoing + ", in:" + incoming + ", loop:" + loop + ']';
        }

        public boolean isEmpty() {
            return outgoing == 0 && incoming == 0 && loop == 0;
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
        EagerDegrees that = (EagerDegrees) o;
        int[] types = types();
        int[] otherTypes = that.types();
        Arrays.sort(types);
        Arrays.sort(otherTypes);
        if (!Arrays.equals(types, otherTypes)) {
            return false;
        }
        for (int type : types) {
            if (!findDegree(type).equals(that.findDegree(type))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int type : types()) {
            result = result * 31 + findDegree(type).hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder degrees = new StringBuilder();
        for (int type : types()) {
            degrees.append(degrees.length() > 0 ? ", " : "")
                    .append(":")
                    .append(type)
                    .append(findDegree(type));
        }
        return format("Degrees{%s}", degrees);
    }
}
