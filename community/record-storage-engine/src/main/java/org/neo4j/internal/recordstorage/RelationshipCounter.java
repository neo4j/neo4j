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
package org.neo4j.internal.recordstorage;

import org.neo4j.internal.batchimport.cache.LongArray;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static java.lang.Math.toIntExact;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

public class RelationshipCounter
{
    public static final Incrementer MANUAL_INCREMENTER = ( array, index ) -> array.set( index, array.get( index ) + 1 );
    private static final StrayRelationship IGNORE_STRAY_RELATIONSHIP = ( s, t, e ) -> {};

    private static final int START = 0;
    private static final int END = 1;
    private static final int SIDES = 2;

    private final NodeLabelsLookup nodeLabelsLookup;
    private final long highLabelId;
    private final long highRelationshipTypeId;
    private final Incrementer incrementer;
    private final long sideSize;
    private final LongArray wildcardCounts;
    private final LongArray labelsCounts;

    public RelationshipCounter( NodeLabelsLookup nodeLabelsLookup, long highLabelId, long highRelationshipTypeId, LongArray wildcardCounts,
            LongArray labelsCounts, Incrementer incrementer )
    {
        this.nodeLabelsLookup = nodeLabelsLookup;
        this.highLabelId = highLabelId;
        this.highRelationshipTypeId = highRelationshipTypeId;
        this.incrementer = incrementer;
        this.sideSize = sideSize( highLabelId, highRelationshipTypeId );
        this.wildcardCounts = wildcardCounts;
        this.labelsCounts = labelsCounts;
    }

    public void process( RelationshipRecord relationship )
    {
        process( relationship, IGNORE_STRAY_RELATIONSHIP );
    }

    public void process( RelationshipRecord relationship, StrayRelationship strayRelationshipReporter )
    {
        process( relationship, strayRelationshipReporter, true, true );
    }

    public void process( RelationshipRecord relationship, StrayRelationship strayRelationshipReporter, boolean processStartNode, boolean processEndNode )
    {
        // Below is logic duplication of CountsState#addRelationship
        processRelationshipTypeCounts( relationship, strayRelationshipReporter );
        processRelationshipNodeCounts( relationship, strayRelationshipReporter, processStartNode, processEndNode );
    }

    public void processRelationshipNodeCounts( RelationshipRecord relationship, StrayRelationship strayRelationshipReporter, boolean processStartNode,
            boolean processEndNode )
    {
        int type = relationship.getType();
        if ( processStartNode )
        {
            for ( long startNodeLabelId : nodeLabelsLookup.nodeLabels( relationship.getFirstNode() ) )
            {
                if ( startNodeLabelId == -1 )
                {   // We reached the end of it
                    break;
                }

                int labelId = toIntExact( startNodeLabelId );
                increment( labelsCounts, labelId, ANY_RELATIONSHIP_TYPE, START, strayRelationshipReporter );
                increment( labelsCounts, labelId, type, START, strayRelationshipReporter );
            }
        }
        if ( processEndNode )
        {
            for ( long endNodeLabelId : nodeLabelsLookup.nodeLabels( relationship.getSecondNode() ) )
            {
                if ( endNodeLabelId == -1 )
                {   // We reached the end of it
                    break;
                }

                int labelId = toIntExact( endNodeLabelId );
                increment( labelsCounts, labelId, ANY_RELATIONSHIP_TYPE, END, strayRelationshipReporter );
                increment( labelsCounts, labelId, type, END, strayRelationshipReporter );
            }
        }
    }

    public void processRelationshipTypeCounts( RelationshipRecord relationship, StrayRelationship strayRelationshipReporter )
    {
        int type = relationship.getType();
        incrementer.increment( wildcardCounts, highRelationshipTypeId );
        if ( isValidRelationshipTypeId( type ) )
        {
            incrementer.increment( wildcardCounts, type );
        }
        else
        {
            strayRelationshipReporter.report( ANY_LABEL, type, ANY_LABEL );
        }
    }

    private long arrayIndex( long labelId, long relationshipTypeId, long side )
    {
        return (side * sideSize) + (labelId * (highRelationshipTypeId + 1) + relationshipTypeId);
    }

    private void increment( LongArray counts, int labelId, int relationshipTypeId, long side, StrayRelationship strayRelationshipReporter )
    {
        if ( isValidLabelId( labelId ) && isValidRelationshipTypeId( relationshipTypeId ) )
        {
            long index = arrayIndex( labelPos( labelId ), relationshipTypePos( relationshipTypeId ), side );
            incrementer.increment( counts, index );
        }
        else
        {
            int startLabelId = side == START ? labelId : ANY_LABEL;
            int endLabelId = side == START ? ANY_LABEL : labelId;
            strayRelationshipReporter.report( startLabelId, relationshipTypeId, endLabelId );
        }
    }

    public static long wildcardCountsLength( long highRelationshipTypeId )
    {
        // Make room for high id + 1 since we need that extra slot for the ANY counts
        return highRelationshipTypeId + 1;
    }

    public static long labelsCountsLength( long highLabelId, long highRelationshipTypeId )
    {
        return sideSize( highLabelId, highRelationshipTypeId ) * SIDES;
    }

    private static long sideSize( long highLabelId, long highRelationshipTypeId )
    {
        // Make room for high id + 1 since we need that extra slot for the ANY counts
        return (highLabelId + 1) * (highRelationshipTypeId + 1);
    }

    public long startLabelCount( int labelId, int typeId )
    {
        return labelsCounts.get( arrayIndex( labelPos( labelId ), relationshipTypePos( typeId ), START ) );
    }

    public long endLabelCount( int labelId, int typeId )
    {
        return labelsCounts.get( arrayIndex( labelPos( labelId ), relationshipTypePos( typeId ), END ) );
    }

    private void setStartLabelCount( int labelId, int typeId, long count )
    {
        labelsCounts.set( arrayIndex( labelPos( labelId ), relationshipTypePos( typeId ), START ), count );
    }

    private void setEndLabelCount( int labelId, int typeId, long count )
    {
        labelsCounts.set( arrayIndex( labelPos( labelId ), relationshipTypePos( typeId ), END ), count );
    }

    private long relationshipTypePos( int typeId )
    {
        return typeId == ANY_RELATIONSHIP_TYPE ? highRelationshipTypeId : typeId;
    }

    private long labelPos( int labelId )
    {
        return labelId == ANY_LABEL ? highLabelId : labelId;
    }

    public boolean isValid( int startLabelId, int relationshipTypeId, int endLabelId )
    {
        return isValidLabelId( startLabelId ) && isValidRelationshipTypeId( relationshipTypeId ) && isValidLabelId( endLabelId );
    }

    private boolean isValidLabelId( int labelId )
    {
        return labelId == ANY_LABEL || (labelId >= 0 && labelId < highLabelId);
    }

    private boolean isValidRelationshipTypeId( int relationshipTypeId )
    {
        return relationshipTypeId == ANY_RELATIONSHIP_TYPE || (relationshipTypeId >= 0 && relationshipTypeId < highRelationshipTypeId);
    }

    public long get( int startLabelId, int relationshipTypeId, int endLabelId )
    {
        if ( startLabelId == ANY_LABEL && endLabelId == ANY_LABEL )
        {
            return wildcardCounts.get( relationshipTypePos( relationshipTypeId ) );
        }
        else if ( startLabelId == ANY_LABEL )
        {
            return endLabelCount( endLabelId, relationshipTypeId );
        }
        else if ( endLabelId == ANY_LABEL )
        {
            return startLabelCount( startLabelId, relationshipTypeId );
        }
        else
        {
            throw new UnsupportedOperationException(
                    "Either start or end label expected to by ANY and the other a real id, was start:" + startLabelId + ", end:" + endLabelId );
        }
    }

    public void set( int startLabelId, int relationshipTypeId, int endLabelId, long count )
    {
        if ( startLabelId == ANY_LABEL && endLabelId == ANY_LABEL )
        {
            wildcardCounts.set( relationshipTypePos( relationshipTypeId ), count );
        }
        else if ( startLabelId == ANY_LABEL )
        {
            setEndLabelCount( endLabelId, relationshipTypeId, count );
        }
        else
        {
            setStartLabelCount( startLabelId, relationshipTypeId, count );
        }
    }

    public interface Incrementer
    {
        void increment( LongArray array, long index );
    }

    public interface StrayRelationship
    {
        void report( int startLabel, int type, int endLabel );
    }

    public interface NodeLabelsLookup
    {
        long[] nodeLabels( long nodeId );
    }
}
