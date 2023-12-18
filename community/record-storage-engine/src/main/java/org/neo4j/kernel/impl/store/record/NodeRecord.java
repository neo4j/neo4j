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
package org.neo4j.kernel.impl.store.record;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.neo4j.storageengine.api.Mask;

import static java.util.Collections.emptyList;
import static org.neo4j.internal.helpers.collection.Iterables.filter;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

public class NodeRecord extends PrimitiveRecord
{
    public static final long SHALLOW_SIZE = shallowSizeOfInstance( NodeRecord.class );
    private long nextRel;
    private long labels;
    private List<DynamicRecord> dynamicLabelRecords;
    private boolean isLight;
    private boolean dense;

    public NodeRecord( long id )
    {
        super( id );
    }

    public NodeRecord( NodeRecord other )
    {
        super( other );
        this.nextRel = other.nextRel;
        this.labels = other.labels;
        if ( other.dynamicLabelRecords.isEmpty() )
        {
            this.dynamicLabelRecords = emptyList();
        }
        else
        {
            this.dynamicLabelRecords = new ArrayList<>( other.dynamicLabelRecords.size() );
            for ( DynamicRecord labelRecord : other.dynamicLabelRecords )
            {
                this.dynamicLabelRecords.add( new DynamicRecord( labelRecord ) );
            }
        }
        this.isLight = other.isLight;
        this.dense = other.dense;
    }

    public NodeRecord initialize( boolean inUse, long nextProp, boolean dense, long nextRel, long labels )
    {
        super.initialize( inUse, nextProp );
        this.nextRel = nextRel;
        this.dense = dense;
        this.labels = labels;
        this.dynamicLabelRecords = emptyList();
        this.isLight = true;
        return this;
    }

    @Override
    public void clear()
    {
        initialize( false, Record.NO_NEXT_PROPERTY.intValue(), false,
                Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_LABELS_FIELD.intValue() );
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
    public void setLabelField( long labels, List<DynamicRecord> dynamicRecords )
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

    public List<DynamicRecord> getDynamicLabelRecords()
    {
        return this.dynamicLabelRecords;
    }

    public Iterable<DynamicRecord> getUsedDynamicLabelRecords()
    {
        return filter( AbstractBaseRecord::inUse, dynamicLabelRecords );
    }

    public boolean isDense()
    {
        return dense;
    }

    public void setDense( boolean dense )
    {
        this.dense = dense;
    }

    @Override
    public String toString( Mask mask )
    {
        String denseInfo = (dense ? "group" : "rel") + "=" + nextRel;
        String lightHeavyInfo = isLight ? "light" :
                                dynamicLabelRecords.isEmpty() ?
                                "heavy" : "heavy,dynlabels=" + dynamicLabelRecords;

        return "Node[" + getId() +
               ",used=" + inUse() +
               ",created=" + isCreated() +
               "," + denseInfo +
               ",prop=" + getNextProp() +
               ",labels=" + parseLabelsField( this ) +
               "," + lightHeavyInfo +
               secondaryUnitToString() + "]";
    }

    @Override
    public void setIdTo( PropertyRecord property )
    {
        property.setNodeId( getId() );
    }

    @Override
    public NodeRecord copy()
    {
        return new NodeRecord( this );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( super.hashCode(), nextRel, labels, dense );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( !super.equals( obj ) )
        {
            return false;
        }
        NodeRecord other = (NodeRecord) obj;
        return nextRel == other.nextRel && labels == other.labels && dense == other.dense;
    }
}
