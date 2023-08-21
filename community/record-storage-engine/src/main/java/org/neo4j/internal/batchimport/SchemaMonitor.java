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
package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.neo4j.values.storable.Value;

public interface SchemaMonitor {
    SchemaMonitor NO_MONITOR = new SchemaMonitor() {
        @Override
        public void property(int propertyKeyId, Object value) {}

        @Override
        public void entityToken(int entityTokenId) {}

        @Override
        public void entityTokens(int[] entityTokenIds) {}

        @Override
        public boolean endOfEntity(long entityId, ViolationVisitor violationVisitor) {
            return true;
        }
    };

    void property(int propertyKeyId, Object value);

    void entityToken(int entityTokenId);

    void entityTokens(int[] entityTokenIds);

    boolean endOfEntity(long entityId, ViolationVisitor violationVisitor);

    interface ViolationVisitor {
        void accept(long entityId, IntList tokens, IntObjectMap<Value> properties, String constraintDescription);
    }
}
