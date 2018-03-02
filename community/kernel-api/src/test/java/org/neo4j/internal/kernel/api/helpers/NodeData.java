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
package org.neo4j.internal.kernel.api.helpers;

import java.util.Map;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.values.storable.Value;

class NodeData
{
    final long id;
    private final long[] labels;
    final Map<Integer,Value> properties;

    NodeData( long id, long[] labels, Map<Integer,Value> properties )
    {
        this.id = id;
        this.labels = labels;
        this.properties = properties;
    }

    LabelSet labelSet()
    {
        return new LabelSet()
        {
            @Override
            public int numberOfLabels()
            {
                return labels.length;
            }

            @Override
            public int label( int offset )
            {
                return labels.length;
            }

            @Override
            public boolean contains( int labelToken )
            {
                for ( long label : labels )
                {
                    if ( label == labelToken )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public long[] all()
            {
                return labels;
            }

            @Override
            public boolean containsAny( int[] labelTokens )
            {
                for ( int labelToken : labelTokens )
                {
                    if ( contains( labelToken ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
