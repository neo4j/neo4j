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
package org.neo4j.storageengine.api.schema;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;

public class SimpleEntityTokenClient extends SimpleEntityClient implements IndexProgressor.EntityTokenClient {
    @Override
    public void initializeQuery(IndexProgressor progressor, int token, IndexOrder order) {
        super.initialize(progressor);
    }

    @Override
    public void initializeQuery(IndexProgressor progressor, int token, LongIterator added, LongSet removed) {
        super.initialize(progressor);
    }

    @Override
    public boolean acceptEntity(long reference, int tokenId) {
        acceptEntity(reference);
        return true;
    }
}
