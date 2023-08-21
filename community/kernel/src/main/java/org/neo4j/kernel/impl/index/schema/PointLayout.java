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

import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.NO_ENTITY_ID;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;

class PointLayout extends IndexLayout<PointKey> {
    private final IndexSpecificSpaceFillingCurveSettings spatialSettings;

    PointLayout(IndexSpecificSpaceFillingCurveSettings spatialSettings) {
        super(false, Layout.namedIdentifier("PL", 1), 0, 1);
        this.spatialSettings = spatialSettings;
    }

    @Override
    public PointKey newKey() {
        return new PointKey(spatialSettings);
    }

    @Override
    public PointKey copyKey(PointKey key, PointKey into) {
        into.copyFrom(key);
        return into;
    }

    @Override
    public int keySize(PointKey key) {
        return key.size();
    }

    @Override
    public void writeKey(PageCursor cursor, PointKey key) {
        key.writeToCursor(cursor);
    }

    @Override
    public void readKey(PageCursor cursor, PointKey into, int keySize) {
        into.readFromCursor(cursor, keySize);
    }

    @Override
    public void initializeAsLowest(PointKey key) {
        // none of the parameters matter as this layout does not support composite keys
        // and it supports just one type
        key.initValueAsLowest(-1, null);
    }

    @Override
    public void initializeAsHighest(PointKey key) {
        // none of the parameters matter as this layout does not support composite keys
        // and it supports just one type
        key.initValueAsHighest(-1, null);
    }

    @Override
    public void minimalSplitter(PointKey left, PointKey right, PointKey into) {
        into.copyFrom(right);
        if (left.compareValueTo(right) != 0) {
            // Since the point value is enough to serve as minimal splitter, we remove the entity id
            // which will serve as divider if the point values are equal.
            into.setEntityId(NO_ENTITY_ID);
        }
    }

    IndexSpecificSpaceFillingCurveSettings getSpaceFillingCurveSettings() {
        return spatialSettings;
    }
}
