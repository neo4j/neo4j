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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageEngineIndexingBehaviour;

/**
 * {@link DefaultRelationshipTypeIndexCursor} which is relationship-based, i.e. the IDs driving the cursor are relationship IDs.
 * @see StorageEngineIndexingBehaviour
 */
public class DefaultRelationshipBasedRelationshipTypeIndexCursor extends DefaultRelationshipTypeIndexCursor {
    private final DefaultRelationshipScanCursor relationshipScanCursor;

    DefaultRelationshipBasedRelationshipTypeIndexCursor(
            CursorPool<DefaultRelationshipTypeIndexCursor> pool, DefaultRelationshipScanCursor relationshipScanCursor) {
        super(pool);
        this.relationshipScanCursor = relationshipScanCursor;
    }

    @Override
    boolean allowedToSeeEntity(AccessMode accessMode, long entityReference) {
        if (accessMode.allowsTraverseAllRelTypes()) {
            return true;
        }
        readEntity(read -> read.singleRelationship(entityReference, relationshipScanCursor));
        return relationshipScanCursor.next();
    }

    @Override
    public void source(NodeCursor cursor) {
        checkReadFromStore();
        read.singleNode(relationshipScanCursor.sourceNodeReference(), cursor);
    }

    @Override
    public void target(NodeCursor cursor) {
        checkReadFromStore();
        read.singleNode(relationshipScanCursor.targetNodeReference(), cursor);
    }

    @Override
    public long sourceNodeReference() {
        checkReadFromStore();
        return relationshipScanCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference() {
        checkReadFromStore();
        return relationshipScanCursor.targetNodeReference();
    }

    @Override
    public long relationshipReference() {
        return entityReference();
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        checkReadFromStore();
        relationshipScanCursor.properties(cursor, selection);
    }

    @Override
    public Reference propertiesReference() {
        checkReadFromStore();
        return relationshipScanCursor.propertiesReference();
    }

    @Override
    public boolean readFromStore() {
        if (relationshipScanCursor.relationshipReference() == entity) {
            // A security check, or a previous call to this method for this relationship already seems to have loaded
            // this relationship
            return true;
        }
        relationshipScanCursor.single(entity, read);
        return relationshipScanCursor.next();
    }

    private void checkReadFromStore() {
        if (relationshipScanCursor.relationshipReference() != entity) {
            throw new IllegalStateException("Relationship hasn't been read from store");
        }
    }

    @Override
    public void release() {
        relationshipScanCursor.close();
        relationshipScanCursor.release();
    }
}
