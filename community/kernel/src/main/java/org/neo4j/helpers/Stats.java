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
package org.neo4j.helpers;

/**
 * @deprecated This class will be removed in the next major release.
 */
@Deprecated
public class Stats
{
    private final String name;
    protected int count;
    protected long total;
    protected long high;
    protected long low;
    
    public Stats( String name )
    {
        this.name = name;
    }
    
    public int add( long value )
    {
        total += value;
        if ( value < low ) low = value;
        if ( value > high ) high = value;
        return ++count;
    }
    
    public long high()
    {
        return high;
    }
    
    public long low()
    {
        return low;
    }
    
    public long average()
    {
        return total/count;
    }
    
    public double preciseAverage()
    {
        return (double)total/(double)count;
    }
    
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( "Stats(" + name + ", " + count + "):" );
        builder.append( "\n  total: " + total );
        builder.append( "\n  avg:   " + average() );
        builder.append( "\n  high:  " + high );
        builder.append( "\n  low:   " + low );
        return builder.toString();
    }
}
