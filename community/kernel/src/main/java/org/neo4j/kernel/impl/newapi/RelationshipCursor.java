/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.internal.kernel.api.RelationshipDataAccessor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

abstract class RelationshipCursor extends RelationshipRecord implements RelationshipDataAccessor
{
    Read read;

    RelationshipCursor()
    {
        super( NO_ID );
    }

    @Override
    public long relationshipReference()
    {
        return getId();
    }

    @Override
    public int label()
    {
        return getType();
    }

    @Override
    public boolean hasProperties()
    {
        return nextProp != (long) PropertyCursor.NO_ID;
    }

    @Override
    public void source( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        read.singleNode( sourceNodeReference(), cursor );
    }

    @Override
    public void target( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        read.singleNode( targetNodeReference(), cursor );
    }

    @Override
    public void properties( org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        read.nodeProperties( propertiesReference(), cursor );
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
}
