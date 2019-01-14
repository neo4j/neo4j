/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.configuration;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.helpers.collection.Pair;

import static java.util.stream.Collectors.toList;

import static org.neo4j.helpers.collection.Pair.pair;

public class ConfigValues implements Function<String, String>
{
    private final Map<String, String> raw;

    public ConfigValues( Map<String,String> raw )
    {
        this.raw = raw;
    }

    @Override
    public String apply( String s )
    {
        return raw.get( s );
    }

    public List<Pair<String,String>> rawConfiguration()
    {
        return raw.entrySet().stream()
                .map( e -> pair( e.getKey(), e.getValue() ) )
                .collect( toList() );
    }
}
