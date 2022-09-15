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
package org.neo4j.internal.schema;

import java.util.Collections;
import java.util.Iterator;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public interface SchemaRulesAccessor {
    /**
     * Default implementation of a {@link SchemaRulesAccessor} that returns no {@link IndexDescriptor}s
     */
    SchemaRulesAccessor NO_RULES = new SchemaRulesAccessor() {
        @Override
        public Iterator<IndexDescriptor> indexesGetAll(StoreCursors storeCursors) {
            return Collections.emptyIterator();
        }

        @Override
        public IndexDescriptor[] indexesGetForDescriptor(SchemaDescriptor descriptor, StoreCursors storeCursors) {
            return new IndexDescriptor[0];
        }

        @Override
        public ConstraintDescriptor[] constraintsGetForDescriptor(
                ConstraintDescriptor descriptor, StoreCursors storeCursors) {
            return new ConstraintDescriptor[0];
        }
    };

    /**
     * @param storeCursors the store cursors
     * @return an iterator for all the current index descriptors
     */
    Iterator<IndexDescriptor> indexesGetAll(StoreCursors storeCursors);

    /**
     * Get the index descriptors that match for a given {@link SchemaDescriptor}
     * @param descriptor the schema descriptor
     * @param storeCursors the store cursors
     * @return the matching index descriptors
     */
    IndexDescriptor[] indexesGetForDescriptor(SchemaDescriptor descriptor, StoreCursors storeCursors);

    /**
     * Get the constraint rules that match for a given {@link ConstraintDescriptor}
     * @param descriptor the constraint descriptor
     * @param storeCursors the store cursors
     * @return the matching constraint descriptors
     */
    ConstraintDescriptor[] constraintsGetForDescriptor(ConstraintDescriptor descriptor, StoreCursors storeCursors);
}
