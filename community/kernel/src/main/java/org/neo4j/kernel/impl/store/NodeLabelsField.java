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
package org.neo4j.kernel.impl.store;

import org.neo4j.kernel.impl.store.record.NodeRecord;

/**
 * Logic for parsing and constructing {@link NodeRecord#getLabelField()} and dynamic label
 * records in {@link NodeRecord#getDynamicLabelRecords()} from label ids.
 * <p>
 * Each node has a label field of 5 bytes, where labels will be stored, if sufficient space
 * (max bits required for storing each label id is considered). If not then the field will
 * point to a dynamic record where the labels will be stored in the format of an array property.
 * <p>
 * [hhhh,bbbb][bbbb,bbbb][bbbb,bbbb][bbbb,bbbb][bbbb,bbbb]
 * h: header
 * - 0x0<=h<=0x7 (leaving high bit reserved): number of in-lined labels in the body
 * - 0x8: body will be a pointer to first dynamic record in node-labels dynamic store
 * b: body
 * - 0x0<=h<=0x7 (leaving high bit reserved): bits of this many in-lined label ids
 * - 0x8: pointer to node-labels store
 */
public class NodeLabelsField
{
    public static NodeLabels parseLabelsField( NodeRecord node )
    {
        long labelField = node.getLabelField();
        return fieldPointsToDynamicRecordOfLabels( labelField )
                ? new DynamicNodeLabels( labelField, node )
                : new InlineNodeLabels( labelField, node );
    }

    public static long[] get( NodeRecord node, NodeStore nodeStore )
    {
        return fieldPointsToDynamicRecordOfLabels( node.getLabelField() )
                ? DynamicNodeLabels.get( node, nodeStore )
                : InlineNodeLabels.get( node );
    }

    public static boolean fieldPointsToDynamicRecordOfLabels( long labelField )
    {
        return (labelField & 0x8000000000L) != 0;
    }

    public static long parseLabelsBody( long labelField )
    {
        return labelField & 0xFFFFFFFFFL;
    }

    /**
     * @see NodeRecord
     *
     * @param labelField label field value from a node record
     * @return the id of the dynamic record this label field points to or null if it is an inline label field
     */
    public static long firstDynamicLabelRecordId( long labelField )
    {
        assert fieldPointsToDynamicRecordOfLabels( labelField );
        return parseLabelsBody( labelField );
    }

    /**
     * Checks so that a label id array is sane, i.e. that it's sorted and contains no duplicates.
     */
    public static boolean isSane( long[] labelIds )
    {
        long prev = -1;
        for ( long labelId : labelIds )
        {
            if ( labelId <= prev )
            {
                return false;
            }
            prev = labelId;
        }
        return true;
    }
}
