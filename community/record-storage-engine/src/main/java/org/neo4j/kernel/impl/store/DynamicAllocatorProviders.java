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
package org.neo4j.kernel.impl.store;

import static org.neo4j.kernel.impl.store.StoreType.STORE_TYPES;

public class DynamicAllocatorProviders {

    public static DynamicAllocatorProvider nonTransactionalAllocator(NeoStores neoStores) {
        return new DetachedDynamicAllocatorProvider(neoStores);
    }

    private static class DetachedDynamicAllocatorProvider implements DynamicAllocatorProvider {
        private final StandardDynamicRecordAllocator[] dynamicAllocators =
                new StandardDynamicRecordAllocator[STORE_TYPES.length];
        private final NeoStores neoStores;

        private DetachedDynamicAllocatorProvider(NeoStores neoStores) {
            this.neoStores = neoStores;
        }

        @Override
        public DynamicRecordAllocator allocator(StoreType type) {
            StandardDynamicRecordAllocator allocator = dynamicAllocators[type.ordinal()];
            if (allocator != null) {
                return allocator;
            }

            var recordStore = neoStores.getRecordStore(type);
            var newAllocator = new StandardDynamicRecordAllocator(
                    cursorContext -> recordStore.getIdGenerator().nextId(cursorContext),
                    neoStores.getRecordStore(type).getRecordDataSize());
            dynamicAllocators[type.ordinal()] = newAllocator;
            return newAllocator;
        }
    }
}
