/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api.schema;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;

/**
 * Reader of a label scan store which contains label-->nodes mappings.
 */
public interface LabelScanReader extends Resource
{
    /**
     * Used as a marker to ignore the "fromId" in calls to {@link #nodesWithAnyOfLabels(long, int[])}.
     */
    long NO_ID = -1;

    /**
     * @param labelId label token id.
     * @return node ids with the given {@code labelId}.
     */
    PrimitiveLongResourceIterator nodesWithLabel( int labelId );

    /**
     * Sets the client up for a label scan on <code>labelId</code>
     *
     * @param client the client to communicate with
     * @param labelId label token id
     */
    void nodesWithLabel( IndexProgressor.NodeLabelClient client, int labelId );

    /**
     * @param labelIds label token ids.
     * @return node ids with any of the given label ids.
     */
    default PrimitiveLongResourceIterator nodesWithAnyOfLabels( int[] labelIds )
    {
        return nodesWithAnyOfLabels( NO_ID, labelIds );
    }

    /**
     * @param fromId entity id to start at, exclusive, i.e. the given {@code fromId} will not be included in the result.
     * @param labelIds label token ids.
     * @return node ids with any of the given label ids.
     */
    PrimitiveLongResourceIterator nodesWithAnyOfLabels( long fromId, int[] labelIds );

    /**
     * @param labelIds label token ids.
     * @return node ids with all of the given label ids.
     */
    PrimitiveLongResourceIterator nodesWithAllLabels( int[] labelIds );
}
