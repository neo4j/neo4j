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
import java.util.Collection;

import org.neo4j.helpers.Pair;

import static java.lang.String.format;

/**
 * Generic implementation for providing {@link Stat statistics}.
 */
public class GenericStatsProvider implements StatsProvider
{
    private final Collection<Pair<Key,Stat>> stats = new ArrayList<>();

    public void add( Key key, Stat stat )
    {
        this.stats.add( Pair.of( key, stat ) );
    }

    @Override
    public Stat stat( Key key )
    {
        for ( Pair<Key,Stat> stat1 : stats )
        {
            if ( stat1.first().name().equals( key.name() ) )
            {
                return stat1.other();
            }
        }
        return null;
    }

    @Override
    public Key[] keys()
    {
        Key[] keys = new Key[stats.size()];
        int i = 0;
        for ( Pair<Key,Stat> stat : stats )
        {
            keys[i++] = stat.first();
        }
        return keys;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for ( Pair<Key,Stat> stat : stats )
        {
            builder.append( builder.length() > 0 ? ", " : "" )
                    .append( format( "%s: %s", stat.first().shortName(), stat.other() ) );
        }
        return builder.toString();
    }
}
