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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import org.eclipse.collections.api.factory.Sets;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;

class RangeBlockBasedIndexPopulatorUpdatesTest extends GenericBlockBasedIndexPopulatorUpdatesTest<RangeKey> {
    @Override
    IndexType indexType() {
        return IndexType.RANGE;
    }

    @Override
    RangeBlockBasedIndexPopulator instantiatePopulator(IndexDescriptor indexDescriptor) throws IOException {
        Config config = Config.defaults();
        RangeLayout layout = new RangeLayout(1);
        RangeBlockBasedIndexPopulator populator = new RangeBlockBasedIndexPopulator(
                databaseIndexContext,
                indexFiles,
                layout,
                indexDescriptor,
                false,
                heapBufferFactory((int) kibiBytes(40)),
                config,
                INSTANCE,
                tokenNameLookup,
                BlockBasedIndexPopulator.NO_MONITOR,
                Sets.immutable.empty());
        populator.create();
        return populator;
    }
}
