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
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

abstract class RelationshipCursor extends RelationshipRecord implements RelationshipDataAccessor, RelationshipVisitor<RuntimeException>
{
    private boolean hasChanges;
    private boolean checkHasChanges;
    private PropertyCursor propertyCursor;
    final DefaultCursors pool;
    Read read;

    RelationshipCursor( DefaultCursors pool )
    {
        super( NO_ID );
        this.pool = pool;
    }

    protected void init( Read read )
    {
        this.read = read;
        this.checkHasChanges = true;
    }

    @Override
    public long relationshipReference()
    {
        return getId();
    }

    @Override
    public int type()
    {
        return getType();
    }

    @Override
    public boolean hasProperties()
    {
        if ( read.hasTxStateWithChanges() )
        {
            PropertyCursor cursor = propertyCursor();
            properties( cursor );
            return cursor.next();
        }
        else
        {
            return nextProp != DefaultPropertyCursor.NO_ID;
        }
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
        return getFirstNode();
    }

    @Override
    public long targetNodeReference()
    {
        return getSecondNode();
    }

    @Override
    public long propertiesReference()
    {
        return getNextProp();
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

    // Load transaction state using RelationshipVisitor
    void loadFromTxState( long reference )
    {
        read.txState().relationshipVisit( reference, this );
    }

    // used to visit transaction state
    @Override
    public void visit( long relationshipId, int typeId, long startNodeId, long endNodeId )
    {
        setId( relationshipId );
        initialize( true, NO_ID, startNodeId, endNodeId, typeId, NO_ID, NO_ID, NO_ID, NO_ID, false, false );
    }

    private PropertyCursor propertyCursor()
    {
        if ( propertyCursor == null )
        {
            propertyCursor = pool.allocatePropertyCursor();
        }
        return propertyCursor;
    }

    void close()
    {
        if ( propertyCursor != null )
        {
            propertyCursor.close();
            propertyCursor = null;
        }
    }
}
