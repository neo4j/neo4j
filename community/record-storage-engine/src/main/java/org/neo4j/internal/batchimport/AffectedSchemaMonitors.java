/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

class AffectedSchemaMonitors implements Supplier<SchemaMonitor> {
    private final List<AffectedSchemaMonitor> localMonitors = new CopyOnWriteArrayList<>();

    @Override
    public SchemaMonitor get() {
        var localMonitor = new AffectedSchemaMonitor();
        localMonitors.add(localMonitor);
        return localMonitor;
    }

    Set<AffectedSchema> getAffectedSchema() {
        if (localMonitors.isEmpty()) {
            return Collections.emptySet();
        }
        var iterator = localMonitors.iterator();
        var first = iterator.next().affectedSchema;
        while (iterator.hasNext()) {
            first.addAll(iterator.next().affectedSchema);
        }
        return first;
    }

    static class AffectedSchemaMonitor implements SchemaMonitor {
        private final MutableIntSet entityTokens = IntSets.mutable.empty();
        private final MutableIntSet propertyTokens = IntSets.mutable.empty();
        private final Set<AffectedSchema> affectedSchema = new HashSet<>();

        @Override
        public void propertyToken(int propertyKeyId) {
            propertyTokens.add(propertyKeyId);
        }

        @Override
        public void entityToken(int entityTokenId) {
            entityTokens.add(entityTokenId);
        }

        @Override
        public void entityTokens(int[] entityTokenIds) {
            entityTokens.addAll(entityTokenIds);
        }

        @Override
        public void endOfEntity() {
            if (!propertyTokens.isEmpty() || entityTokens.isEmpty()) {
                affectedSchema.add(new AffectedSchema(entityTokens.toArray(), propertyTokens.toArray()));
            }
            propertyTokens.clear();
            entityTokens.clear();
        }
    }

    record AffectedSchema(int[] entityTokens, int[] propertyTokens) implements Serializable {
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AffectedSchema that = (AffectedSchema) o;
            return Arrays.equals(entityTokens, that.entityTokens) && Arrays.equals(propertyTokens, that.propertyTokens);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(entityTokens);
            result = 31 * result + Arrays.hashCode(propertyTokens);
            return result;
        }

        @Override
        public String toString() {
            return "AffectedSchema{" + "entityTokens="
                    + Arrays.toString(entityTokens) + ", propertyTokens="
                    + Arrays.toString(propertyTokens) + '}';
        }
    }
}
