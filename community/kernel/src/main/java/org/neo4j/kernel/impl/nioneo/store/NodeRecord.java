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
package org.neo4j.kernel.impl.nioneo.store;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NodeRecord extends PrimitiveRecord
{
    private final long committedNextRel;
    private long nextRel;
    private long labels;
    private Collection<DynamicRecord> dynamicLabelRecords = emptyList();
    private boolean isLight = true;

    public NodeRecord( long id, long nextRel, long nextProp )
    {
        super( id, nextProp );
        this.committedNextRel = this.nextRel = nextRel;
    }
    
    public long getNextRel()
    {
        return nextRel;
    }

    public void setNextRel( long nextRel )
    {
        this.nextRel = nextRel;
    }

    public long getCommittedNextRel()
    {
        return isCreated() ? Record.NO_NEXT_RELATIONSHIP.intValue() : committedNextRel;
    }
    
    /**
     * Sets the label field to an in-lined field, no dynamic records were changed by doing this.
     * 
     * @param labels this will be in-lined labels.
     */
    public void setLabelField( long labels )
    {
        this.labels = labels;
        this.dynamicLabelRecords = emptyList();
        this.isLight = true;
    }
    
    /**
     * Sets the label field to a pointer to the first changed dynamic record. All changed
     * dynamic records by doing this are supplied here.
     * 
     * @param labels this will be either in-lined labels, or an id where to get the labels
     * @param changedDynamicRecords all changed dynamic records by doing this.
     */
    public void setLabelField( long labels, Collection<DynamicRecord> changedDynamicRecords )
    {
        this.labels = labels;
        this.dynamicLabelRecords = changedDynamicRecords;
        this.isLight = false;
    }
    
    public long getLabelField()
    {
        return this.labels;
    }
    
    public boolean isLight()
    {
        return isLight;
    }
    
    public Collection<DynamicRecord> getDynamicLabelRecords()
    {
        return this.dynamicLabelRecords;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( "Node[" ).append( getId() )
                .append( ",used=" ).append( inUse() )
                .append( ",rel=" ).append( nextRel )
                .append( ",prop=" ).append( getNextProp() )
                .append( ",labels=" ).append( getLabelField() )
                .append( "," ).append( isLight ? "light" : "heavy" );
        if ( !isLight && !dynamicLabelRecords.isEmpty() )
            builder.append( ",dynlabels=" ).append( dynamicLabelRecords );
        return builder.append( "]" ).toString();
    }

    @Override
    public void setIdTo( PropertyRecord property )
    {
        property.setNodeId( getId() );
    }
    
    @Override
    public NodeRecord clone()
    {
        NodeRecord clone = new NodeRecord( getId(), getCommittedNextRel(), getCommittedNextProp() );
        clone.setNextProp( getNextProp() );
        clone.nextRel = nextRel;
        clone.labels = labels;
        clone.isLight = isLight;

        if( dynamicLabelRecords.size() > 0 )
        {
            List<DynamicRecord> clonedLabelRecords = new ArrayList<DynamicRecord>(dynamicLabelRecords.size());
            for ( DynamicRecord labelRecord : dynamicLabelRecords )
                clonedLabelRecords.add( labelRecord.clone() );
            clone.dynamicLabelRecords = clonedLabelRecords;
        }
        return clone;
    }
}
