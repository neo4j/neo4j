/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

abstract class RelationshipCursor<STORECURSOR extends StoreRelationshipCursor> implements RelationshipDataAccessor
{
    Read read;
    private boolean hasChanges;
    private boolean checkHasChanges;

    final STORECURSOR storeCursor;

    RelationshipCursor( STORECURSOR storeCursor )
    {
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
        return storeCursor.relationshipReference();
    }

    @Override
    public int type()
    {
        return storeCursor.type();
    }

    @Override
    public boolean hasProperties()
    {
        return storeCursor.hasProperties();
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
        read.relationshipProperties( relationshipReference(), propertiesReference(), cursor );
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
