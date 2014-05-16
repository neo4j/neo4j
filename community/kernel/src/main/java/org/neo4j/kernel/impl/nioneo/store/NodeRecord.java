/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.helpers.Predicate;

public class NodeRecord extends PrimitiveRecord
{
    private long nextRel;
    private long labels;
    private Collection<DynamicRecord> dynamicLabelRecords = emptyList();
    private boolean isLight = true;
    private final boolean committedDense;
    private boolean dense;

    public NodeRecord( long id )
    {
        super( id, Record.NO_NEXT_PROPERTY.intValue() );
        this.committedDense = false;
    }

    public NodeRecord( long id, boolean dense, long nextRel, long nextProp )
    {
        super( id, nextProp );
        this.nextRel = nextRel;
        this.committedDense = this.dense = dense;
    }

    public long getNextRel()
    {
        return nextRel;
    }

    public void setNextRel( long nextRel )
    {
        this.nextRel = nextRel;
    }

    /**
     * Sets the label field to a pointer to the first changed dynamic record. All changed
     * dynamic records by doing this are supplied here.
     *
     * @param labels this will be either in-lined labels, or an id where to get the labels
     * @param dynamicRecords all changed dynamic records by doing this.
     */
    public void setLabelField( long labels, Collection<DynamicRecord> dynamicRecords )
    {
        this.labels = labels;
        this.dynamicLabelRecords = dynamicRecords;

        // Only mark it as heavy if there are dynamic records, since there's a possibility that we just
        // loaded a light version of the node record where this method was called for setting the label field.
        // Keeping it as light in this case would make it possible to load it fully later on.
        this.isLight = dynamicRecords.isEmpty();
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

    public Iterable<DynamicRecord> getUsedDynamicLabelRecords()
    {
        return filter( RECORD_IN_USE, dynamicLabelRecords );
    }

    public boolean isDense()
    {
        return dense;
    }

    public boolean isCommittedDense()
    {
        return committedDense;
    }

    public void setDense( boolean dense )
    {
        this.dense = dense;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( "Node[" ).append( getId() )
                .append( ",used=" ).append( inUse() )
                .append( "," + (dense ? "group" : "rel") + "=" ).append( nextRel )
                .append( ",prop=" ).append( getNextProp() )
                .append( ",labels=" ).append( parseLabelsField( this ) )
                .append( "," ).append( isLight ? "light" : "heavy" );
        if ( !isLight && !dynamicLabelRecords.isEmpty() )
        {
            builder.append( ",dynlabels=" ).append( dynamicLabelRecords );
        }
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
        NodeRecord clone = new NodeRecord( getId(), committedDense, nextRel, getNextProp() );
        clone.labels = labels;
        clone.isLight = isLight;
        clone.setInUse( inUse() );

        if( dynamicLabelRecords.size() > 0 )
        {
            List<DynamicRecord> clonedLabelRecords = new ArrayList<>(dynamicLabelRecords.size());
            for ( DynamicRecord labelRecord : dynamicLabelRecords )
            {
                clonedLabelRecords.add( labelRecord.clone() );
            }
            clone.dynamicLabelRecords = clonedLabelRecords;
        }
        return clone;
    }

    public static final Predicate<DynamicRecord> RECORD_IN_USE = new Predicate<DynamicRecord>()
    {
        @Override
        public boolean accept( DynamicRecord item )
        {
            return item.inUse();
        }
    };

    public void copyFrom( NodeRecord from )
    {
        this.nextRel = from.nextRel;
        this.labels = from.labels;
        this.isLight = from.isLight;
        this.dense = from.dense;
        this.setNextProp( from.getNextProp() );
    }
}
