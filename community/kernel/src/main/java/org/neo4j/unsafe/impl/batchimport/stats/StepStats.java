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
package org.neo4j.unsafe.impl.batchimport.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Provides stats about a {@link Step}.
 */
public class StepStats implements StatsProvider
{
    private final String name;
    private final boolean stillWorking;
    private final Collection<StatsProvider> providers;

    public StepStats( String name, boolean stillWorking, Collection<StatsProvider> providers )
    {
        this.name = name;
        this.stillWorking = stillWorking;
        this.providers = new ArrayList<>( providers );
    }

    public boolean stillWorking()
    {
        return stillWorking;
    }

    @Override
    public Key[] keys()
    {
        Key[] keys = null;
        for ( StatsProvider provider : providers )
        {
            Key[] providerKeys = provider.keys();
            if ( keys == null )
            {
                keys = providerKeys;
            }
            else
            {
                for ( Key providerKey : providerKeys )
                {
                    if ( !arrayContains( keys, providerKey ) )
                    {
                        keys = Arrays.copyOf( keys, keys.length+1 );
                        keys[keys.length-1] = providerKey;
                    }
                }
            }
        }
        return keys;
    }

    private <T> boolean arrayContains( T[] array, T item )
    {
        for ( T arrayItem : array )
        {
            if ( arrayItem.equals( item ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public Stat stat( Key key )
    {
        for ( StatsProvider provider : providers )
        {
            Stat stat = provider.stat( key );
            if ( stat != null )
            {
                return stat;
            }
        }
        return null;
    }

    @Override
    public String toString()
    {
        return toString( DetailLevel.IMPORTANT );
    }

    public String toString( DetailLevel detailLevel )
    {
        StringBuilder builder = new StringBuilder();
        if ( !stillWorking && detailLevel == DetailLevel.BASIC )
        {
            builder.append( " DONE" );
        }

        int i = 0;
        for ( Key key : keys() )
        {
            Stat stat = stat( key );
            if ( detailLevel.ordinal() >= stat.detailLevel().ordinal() )
            {
                builder.append( i++ > 0 ? " " : "" )
                       .append( key.shortName() != null ? key.shortName() + ":" : "" )
                       .append( stat );
            }
        }
        return name + (builder.length() > 0 ? ":" + builder : "");
    }
}
