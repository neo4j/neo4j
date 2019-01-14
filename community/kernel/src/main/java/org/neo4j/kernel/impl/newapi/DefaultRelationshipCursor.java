/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;

abstract class DefaultRelationshipCursor<STORECURSOR extends StorageRelationshipCursor> implements RelationshipDataAccessor
{
    private boolean hasChanges;
    private boolean checkHasChanges;
    final DefaultCursors pool;
    Read read;

    final STORECURSOR storeCursor;

    DefaultRelationshipCursor( DefaultCursors pool, STORECURSOR storeCursor )
    {
        this.pool = pool;
        this.storeCursor = storeCursor;
    }

    protected void init( Read read )
    {
        this.read = read;
        this.checkHasChanges = true;
    }

    @Override
    public long relationshipReference()
    {
        return storeCursor.entityReference();
    }

    @Override
    public int type()
    {
        return storeCursor.type();
    }

    @Override
    public void source( NodeCursor cursor )
    {
        read.singleNode( sourceNodeReference(), cursor );
    }

    @Override
    public void target( NodeCursor cursor )
    {
        read.singleNode( targetNodeReference(), cursor );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        ((DefaultPropertyCursor) cursor).initRelationship( relationshipReference(), propertiesReference(), read, read );
    }

    @Override
    public long sourceNodeReference()
    {
        return storeCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference()
    {
        return storeCursor.targetNodeReference();
    }

    @Override
    public long propertiesReference()
    {
        return storeCursor.propertiesReference();
    }

    protected abstract void collectAddedTxStateSnapshot();

    /**
     * RelationshipCursor should only see changes that are there from the beginning
     * otherwise it will not be stable.
     */
    protected boolean hasChanges()
    {
        if ( checkHasChanges )
        {
            hasChanges = read.hasTxStateWithChanges();
            if ( hasChanges )
            {
                collectAddedTxStateSnapshot();
            }
            checkHasChanges = false;
        }

        return hasChanges;
    }
}
