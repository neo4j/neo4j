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
package org.neo4j.storageengine.util;

import org.neo4j.io.IOUtils;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageEntityScanCursor;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

/**
 * A relationship cursor which is driven by node references and where calls to {@link #next()} first moves to the selected node
 * and from there gets its relevant relationships.
 */
public class StorageRelationshipByNodeScanCursor
        implements StorageEntityScanCursor<AllNodeScan>, StorageRelationshipCursor {
    private final StorageNodeCursor nodeCursor;
    private final StorageRelationshipTraversalCursor relationshipCursor;
    private final RelationshipSelection relationshipSelection;
    private boolean newNode;

    public StorageRelationshipByNodeScanCursor(
            StorageNodeCursor nodeCursor,
            StorageRelationshipTraversalCursor relationshipCursor,
            RelationshipSelection relationshipSelection) {
        this.nodeCursor = nodeCursor;
        this.relationshipCursor = relationshipCursor;
        this.relationshipSelection = relationshipSelection;
    }

    @Override
    public boolean next() {
        if (newNode) {
            newNode = false;
            if (!nodeCursor.next()) {
                return false;
            }
            nodeCursor.relationships(relationshipCursor, relationshipSelection);
        }
        return relationshipCursor.next();
    }

    @Override
    public void reset() {
        nodeCursor.reset();
        relationshipCursor.reset();
        newNode = false;
    }

    @Override
    public void setForceLoad() {
        nodeCursor.setForceLoad();
        relationshipCursor.setForceLoad();
    }

    @Override
    public void close() {
        IOUtils.closeAllUnchecked(nodeCursor, relationshipCursor);
    }

    @Override
    public boolean hasProperties() {
        return relationshipCursor.hasProperties();
    }

    @Override
    public Reference propertiesReference() {
        return relationshipCursor.propertiesReference();
    }

    @Override
    public void properties(StoragePropertyCursor propertyCursor, PropertySelection selection) {
        relationshipCursor.properties(propertyCursor, selection);
    }

    @Override
    public long entityReference() {
        return relationshipCursor.entityReference();
    }

    @Override
    public void scan() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean scanBatch(AllNodeScan scan, long sizeHint) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void single(long reference) {
        // This is the node we're targeting
        nodeCursor.single(reference);
        newNode = true;
    }

    @Override
    public int type() {
        return relationshipCursor.type();
    }

    @Override
    public long sourceNodeReference() {
        return relationshipCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference() {
        return relationshipCursor.targetNodeReference();
    }
}
