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

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;

class NodeCursor extends NodeRecord implements org.neo4j.internal.kernel.api.NodeCursor
{
    private final Read read;
    private final RecordCursor<DynamicRecord> labelCursor;
    private long next;
    private long highMark;

    NodeCursor( Read read )
    {
        super( -1 );
        this.read = read;
        this.labelCursor = read.labelCursor();
    }

    void scan()
    {
        if ( getId() != NO_ID )
        {
            close();
        }
        next = 0;
        highMark = read.nodeHighMark();
    }

    void single( long reference )
    {
        if ( getId() != NO_ID )
        {
            close();
        }
        next = reference;
        highMark = NO_ID;
    }

    @Override
    public long nodeReference()
    {
        return getId();
    }

    @Override
    public LabelSet labels()
    {
        return new Labels( NodeLabelsField.get( this, labelCursor ) );
    }

    @Override
    public boolean hasProperties()
    {
        return nextProp != (long) NO_ID;
    }

    @Override
    public void relationships( RelationshipGroupCursor cursor )
    {
        read.relationshipGroups( nodeReference(), relationshipGroupReference(), cursor );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        read.nodeProperties( propertiesReference(), cursor );
    }

    @Override
    public long relationshipGroupReference()
    {
        // use negatives to encode the fact that this is not a proper group reference,
        // although not -1, because it is special
        return isDense() ? getNextRel() : -2 - getNextRel();
    }

    @Override
    public long propertiesReference()
    {
        return getNextProp();
    }

    @Override
    public boolean next()
    {
        if ( next == NO_ID )
        {
            close();
            return false;
        }
        read.node( this, next++ );
        if ( next > highMark )
        {
            if ( highMark == NO_ID )
            {
                next = NO_ID;
            }
            else
            {
                highMark = read.nodeHighMark();
                if ( next > highMark )
                {
                    next = NO_ID;
                }
            }
        }
        return true;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        setId( next = NO_ID );
    }

    @Override
    public void outgoingRelationships( RelationshipGroupCursor groups, RelationshipTraversalCursor relationships )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void incomingRelationships( RelationshipGroupCursor groups, RelationshipTraversalCursor relationships )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void allRelationships( RelationshipGroupCursor groups, RelationshipTraversalCursor relationships )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
