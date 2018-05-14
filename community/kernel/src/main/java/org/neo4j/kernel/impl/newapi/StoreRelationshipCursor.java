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
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

abstract class StoreRelationshipCursor extends RelationshipRecord implements RelationshipVisitor<RuntimeException>
{
    Read read;

    StoreRelationshipCursor()
    {
        super( NO_ID );
    }

    protected void init( Read read )
    {
        this.read = read;
    }

    public long relationshipReference()
    {
        return getId();
    }

    public int type()
    {
        return getType();
    }

    public boolean hasProperties()
    {
        return nextProp != NO_ID;
    }

    public void source( NodeCursor cursor )
    {
        read.singleNode( sourceNodeReference(), cursor );
    }

    public void target( NodeCursor cursor )
    {
        read.singleNode( targetNodeReference(), cursor );
    }

    public void properties( PropertyCursor cursor )
    {
        read.relationshipProperties( relationshipReference(), propertiesReference(), cursor );
    }

    public long sourceNodeReference()
    {
        return getFirstNode();
    }

    public long targetNodeReference()
    {
        return getSecondNode();
    }

    public long propertiesReference()
    {
        return getNextProp();
    }

    // used to visit transaction state
    @Override
    public void visit( long relationshipId, int typeId, long startNodeId, long endNodeId )
    {
        setId( relationshipId );
        initialize( true, NO_ID, startNodeId, endNodeId, typeId, NO_ID, NO_ID, NO_ID, NO_ID, false, false );
    }
}
