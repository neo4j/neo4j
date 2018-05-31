/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.store.counts.keys;

import static java.lang.Math.toIntExact;

public class CountsKeyFactory
{
    private CountsKeyFactory()
    {
    }

    public static NodeKey nodeKey( long labelId )
    {
        return new NodeKey( toIntExact( labelId ) );
    }

    public static RelationshipKey relationshipKey( long startLabelId, int typeId, long endLabelId )
    {
        return new RelationshipKey( toIntExact( startLabelId ), typeId, toIntExact( endLabelId ) );
    }

    public static IndexStatisticsKey indexStatisticsKey( long indexId )
    {
        return new IndexStatisticsKey( indexId );
    }

    public static IndexSampleKey indexSampleKey( long indexId )
    {
        return new IndexSampleKey( indexId );
    }
}
