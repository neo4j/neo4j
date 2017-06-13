/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.labelscan;

public abstract class NodeLabelRange
{
    public abstract int id();

    public abstract long[] nodes();

    public abstract long[] labels( long nodeId );

    public String toString( String prefix, long[] nodes, long[][] labels )
    {
        StringBuilder result = new StringBuilder( prefix );
        result.append( "; {" );
        for ( int i = 0; i < nodes.length; i++ )
        {
            if ( i != 0 )
            {
                result.append( ", " );
            }
            result.append( "Node[" ).append( nodes[i] ).append( "]: Labels[" );
            String sep = "";
            if ( labels[i] != null )
            {
                for ( long labelId : labels[i] )
                {
                    result.append( sep ).append( labelId );
                    sep = ", ";
                }
            }
            else
            {
                result.append( "null" );
            }
            result.append( "]" );
        }
        return result.append( "}]" ).toString();
    }
}
