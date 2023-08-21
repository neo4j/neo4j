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
package org.neo4j.kernel.impl.store.cursor;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.MAX_TYPE;

import org.neo4j.internal.recordstorage.RecordCursorTypes;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.storageengine.api.cursor.CursorType;

public class CachedStoreCursors extends AbstractCachedStoreCursors {
    private final NeoStores neoStores;

    public CachedStoreCursors(NeoStores neoStores, CursorContext cursorContext) {
        super(cursorContext, MAX_TYPE + 1);
        this.neoStores = neoStores;
    }

    @Override
    public PageCursor writeCursor(CursorType type) {
        return switch (cast(type)) {
            case NODE_CURSOR -> neoStores.getNodeStore().openPageCursorForWriting(0, cursorContext);
            case GROUP_CURSOR -> neoStores.getRelationshipGroupStore().openPageCursorForWriting(0, cursorContext);
            case SCHEMA_CURSOR -> neoStores.getSchemaStore().openPageCursorForWriting(0, cursorContext);
            case RELATIONSHIP_CURSOR -> neoStores.getRelationshipStore().openPageCursorForWriting(0, cursorContext);
            case PROPERTY_CURSOR -> neoStores.getPropertyStore().openPageCursorForWriting(0, cursorContext);
            case DYNAMIC_ARRAY_STORE_CURSOR -> neoStores
                    .getPropertyStore()
                    .getArrayStore()
                    .openPageCursorForWriting(0, cursorContext);
            case DYNAMIC_STRING_STORE_CURSOR -> neoStores
                    .getPropertyStore()
                    .getStringStore()
                    .openPageCursorForWriting(0, cursorContext);
            case DYNAMIC_LABEL_STORE_CURSOR -> neoStores
                    .getNodeStore()
                    .getDynamicLabelStore()
                    .openPageCursorForWriting(0, cursorContext);
            case DYNAMIC_REL_TYPE_TOKEN_CURSOR -> neoStores
                    .getRelationshipTypeTokenStore()
                    .getNameStore()
                    .openPageCursorForWriting(0, cursorContext);
            case REL_TYPE_TOKEN_CURSOR -> neoStores
                    .getRelationshipTypeTokenStore()
                    .openPageCursorForWriting(0, cursorContext);
            case DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR -> neoStores
                    .getPropertyKeyTokenStore()
                    .getNameStore()
                    .openPageCursorForWriting(0, cursorContext);
            case PROPERTY_KEY_TOKEN_CURSOR -> neoStores
                    .getPropertyKeyTokenStore()
                    .openPageCursorForWriting(0, cursorContext);
            case DYNAMIC_LABEL_TOKEN_CURSOR -> neoStores
                    .getLabelTokenStore()
                    .getNameStore()
                    .openPageCursorForWriting(0, cursorContext);
            case LABEL_TOKEN_CURSOR -> neoStores.getLabelTokenStore().openPageCursorForWriting(0, cursorContext);
        };
    }

    @Override
    protected PageCursor createReadCursor(CursorType type) {
        return switch (cast(type)) {
            case NODE_CURSOR -> neoStores.getNodeStore().openPageCursorForReading(0, cursorContext);
            case GROUP_CURSOR -> neoStores.getRelationshipGroupStore().openPageCursorForReading(0, cursorContext);
            case SCHEMA_CURSOR -> neoStores.getSchemaStore().openPageCursorForReading(0, cursorContext);
            case RELATIONSHIP_CURSOR -> neoStores.getRelationshipStore().openPageCursorForReading(0, cursorContext);
            case PROPERTY_CURSOR -> neoStores.getPropertyStore().openPageCursorForReading(0, cursorContext);
            case DYNAMIC_ARRAY_STORE_CURSOR -> neoStores
                    .getPropertyStore()
                    .getArrayStore()
                    .openPageCursorForReading(0, cursorContext);
            case DYNAMIC_STRING_STORE_CURSOR -> neoStores
                    .getPropertyStore()
                    .getStringStore()
                    .openPageCursorForReading(0, cursorContext);
            case DYNAMIC_LABEL_STORE_CURSOR -> neoStores
                    .getNodeStore()
                    .getDynamicLabelStore()
                    .openPageCursorForReading(0, cursorContext);
            case DYNAMIC_REL_TYPE_TOKEN_CURSOR -> neoStores
                    .getRelationshipTypeTokenStore()
                    .getNameStore()
                    .openPageCursorForReading(0, cursorContext);
            case REL_TYPE_TOKEN_CURSOR -> neoStores
                    .getRelationshipTypeTokenStore()
                    .openPageCursorForReading(0, cursorContext);
            case DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR -> neoStores
                    .getPropertyKeyTokenStore()
                    .getNameStore()
                    .openPageCursorForReading(0, cursorContext);
            case PROPERTY_KEY_TOKEN_CURSOR -> neoStores
                    .getPropertyKeyTokenStore()
                    .openPageCursorForReading(0, cursorContext);
            case DYNAMIC_LABEL_TOKEN_CURSOR -> neoStores
                    .getLabelTokenStore()
                    .getNameStore()
                    .openPageCursorForReading(0, cursorContext);
            case LABEL_TOKEN_CURSOR -> neoStores.getLabelTokenStore().openPageCursorForReading(0, cursorContext);
        };
    }

    private static RecordCursorTypes cast(CursorType type) {
        if (type instanceof RecordCursorTypes) {
            return (RecordCursorTypes) type;
        }
        throw new IllegalArgumentException(String.format(
                "%s(%s) is of incorrect type. Expected %s.",
                type, type.getClass().getSimpleName(), RecordCursorTypes.class.getSimpleName()));
    }
}
