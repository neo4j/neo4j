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
package org.neo4j.kernel.impl.nioneo.store.labels;

import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

/**
 * Logic for parsing and constructing {@link NodeRecord#getLabelField()} and dynamic label
 * records in {@link NodeRecord#getDynamicLabelRecords()} from label ids.
 * <p/>
 * Each node has a label field of 5 bytes, where labels will be stored, if sufficient space
 * (max bits required for storing each label id is considered). If not then the field will
 * point to a dynamic record where the labels will be stored in the format of an array property.
 * <p/>
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
        if ( fieldPointsToDynamicRecordOfLabels( labelField ) )
        {
            return new DynamicNodeLabels( labelField, node );
        }
        else
        {
            return new InlineNodeLabels( labelField, node );
        }
    }

    static long parseLabelsBody( long labelsField )
    {
        return labelsField & 0xFFFFFFFFFL;
    }

    private static boolean fieldPointsToDynamicRecordOfLabels( long labelField )
    {
        return (labelField & 0x8000000000L) != 0;
    }

    /**
     * @see NodeRecord
     *
     * @param labelField label field value from a node record
     * @return the id of the dynamic record this label field points to or null if it is an inline label field
     */
    public static Long fieldDynamicLabelRecordId( long labelField )
    {
        if ( fieldPointsToDynamicRecordOfLabels( labelField ) )
        {
            return parseLabelsBody( labelField );
        }
        else
        {
            return null;
        }
    }
}