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
package org.neo4j.kernel.internal.event;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import org.neo4j.graphdb.Relationship;
import org.neo4j.values.storable.Value;

class RelationshipPropertyEntryView extends EntityPropertyEntryView<Relationship> {
    static final long SHALLOW_SIZE = shallowSizeOfInstance(RelationshipPropertyEntryView.class);

    private final Relationship relationship;

    RelationshipPropertyEntryView(Relationship relationship, String key, Value newValue, Value oldValue) {
        super(key, newValue, oldValue);
        this.relationship = relationship;
    }

    @Override
    public Relationship entity() {
        return relationship;
    }

    @Override
    public String toString() {
        return "RelationshipPropertyEntryView{" + "relId="
                + relationship.getId() + ", key='"
                + key + '\'' + ", newValue="
                + newValue + ", oldValue="
                + oldValue + '}';
    }
}
