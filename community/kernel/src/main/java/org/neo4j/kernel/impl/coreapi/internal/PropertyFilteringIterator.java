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
package org.neo4j.kernel.impl.coreapi.internal;

import java.util.Arrays;
import org.neo4j.graphdb.Entity;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.impl.newapi.CursorPredicates;
import org.neo4j.storageengine.api.PropertySelection;

public abstract class PropertyFilteringIterator<
                T extends Entity, TOKEN_CURSOR extends Cursor, ENTITY_CURSOR extends Cursor>
        extends PrefetchingEntityResourceIterator<TOKEN_CURSOR, T> {
    private final TOKEN_CURSOR entityTokenCursor;
    private final ENTITY_CURSOR entityCursor;
    private final PropertyCursor propertyCursor;
    private final PropertyIndexQuery[] queries;
    private final ResourceMonitor resourceMonitor;
    private final PropertySelection propertySelection;

    protected PropertyFilteringIterator(
            TOKEN_CURSOR entityTokenCursor,
            ENTITY_CURSOR entityCursor,
            PropertyCursor propertyCursor,
            CursorEntityFactory<TOKEN_CURSOR, T> entityFactory,
            ResourceMonitor resourceMonitor,
            PropertyIndexQuery[] queries) {
        super(entityTokenCursor, entityFactory);
        this.entityTokenCursor = entityTokenCursor;
        this.entityCursor = entityCursor;
        this.propertyCursor = propertyCursor;
        this.queries = queries;
        this.resourceMonitor = resourceMonitor;
        resourceMonitor.registerCloseableResource(this);
        this.propertySelection = PropertySelection.selection(Arrays.stream(queries)
                .mapToInt(PropertyIndexQuery::propertyKeyId)
                .toArray());
    }

    @Override
    protected long fetchNext() {
        boolean hasNext;
        do {
            hasNext = entityTokenCursor.next();
        } while (hasNext && !hasPropertiesWithValues());

        if (hasNext) {
            return entityReference(entityTokenCursor);
        }
        return NO_ID;
    }

    @Override
    void closeResources() {
        IOUtils.closeAllSilently(entityTokenCursor, entityCursor, propertyCursor);
        resourceMonitor.unregisterCloseableResource(this);
    }

    private boolean hasPropertiesWithValues() {
        singleEntity(entityReference(entityTokenCursor), entityCursor);
        if (entityCursor.next()) {
            properties(entityCursor, propertyCursor, propertySelection);
            return CursorPredicates.propertiesMatch(propertyCursor, queries);
        }
        return false;
    }

    protected abstract long entityReference(TOKEN_CURSOR cursor);

    protected abstract void singleEntity(long id, ENTITY_CURSOR cursor);

    protected abstract void properties(
            ENTITY_CURSOR entityCursor, PropertyCursor propertyCursor, PropertySelection propertySelection);
}
