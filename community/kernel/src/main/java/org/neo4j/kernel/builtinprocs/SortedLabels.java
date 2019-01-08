/*
 * Copyright (c) 2002-2019 "Neo Technology,"
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
package org.neo4j.kernel.builtinprocs;

import java.util.Arrays;

public class SortedLabels
{
    private int[] labels;

    private SortedLabels( int[] labels )
    {
        this.labels = labels;
    }

    public static SortedLabels from( int[] labels )
    {
        Arrays.sort( labels );
        return new SortedLabels( labels );
    }

    private int[] all()
    {
        return labels;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( labels );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof SortedLabels )
        {
            int[] input = ((SortedLabels) obj).all();
            return Arrays.equals( labels, input );
        }
        return false;
    }

    public int numberOfLabels()
    {
        return labels.length;
    }

    public Integer label( int offset )
    {
        return labels[offset];
    }
}
