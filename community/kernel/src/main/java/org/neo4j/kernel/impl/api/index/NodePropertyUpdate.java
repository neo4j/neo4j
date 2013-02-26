/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.kernel.impl.api.UpdateMode;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class NodePropertyUpdate
{
    private final long nodeId;
    private final long propertyKeyId;
    private final Object valueBefore;
    private final Object valueAfter;
    private final UpdateMode updateMode;

    public NodePropertyUpdate( long nodeId, long propertyKeyId, Object valueBefore, Object valueAfter )
    {
        this.nodeId = nodeId;
        this.propertyKeyId = propertyKeyId;
        this.valueBefore = valueBefore;
        this.valueAfter = valueAfter;
        this.updateMode = figureOutUpdateMode( valueBefore, valueAfter );
    }

    private UpdateMode figureOutUpdateMode( Object valueBefore, Object valueAfter )
    {
        boolean beforeSet = valueBefore != null;
        boolean afterSet = valueAfter != null;
        if ( !beforeSet && afterSet )
            return UpdateMode.ADDED;
        if ( beforeSet && afterSet )
            return UpdateMode.CHANGED;
        if ( beforeSet && !afterSet )
            return UpdateMode.REMOVED;
        throw new IllegalArgumentException( "Neither before or after set" );
    }

    public long getNodeId()
    {
        return nodeId;
    }

    public long getPropertyKeyId()
    {
        return propertyKeyId;
    }

    public Object getValueBefore()
    {
        return valueBefore;
    }

    public Object getValueAfter()
    {
        return valueAfter;
    }

    public UpdateMode getUpdateMode()
    {
        return updateMode;
    }

    public boolean hasLabel( long labelId )
    {
        // TODO implement adding label id info
        return true;
    }

    public void apply( IndexWriter indexManipulator )
    {
        switch (getUpdateMode())
        {
            case ADDED:
                indexManipulator.add( getNodeId(), getValueAfter() );
                break;
            case CHANGED:
                indexManipulator.remove( getNodeId(), getValueBefore() );
                indexManipulator.add( getNodeId(), getValueAfter() );
                break;
            case REMOVED:
                indexManipulator.remove( getNodeId(), getValueBefore() );
                break;
        }
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + nodeId + ", prop:" + propertyKeyId + ", before:" + valueBefore +
                ", after:" + valueAfter + "]";
    }
}
