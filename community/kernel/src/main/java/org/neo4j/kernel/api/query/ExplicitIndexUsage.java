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
package org.neo4j.kernel.api.query;

import java.util.HashMap;
import java.util.Map;

public class ExplicitIndexUsage extends IndexUsage
{
    private final String index;
    private final String entityType;

    public ExplicitIndexUsage( String identifier, String index, String entityType )
    {
        super( identifier );
        this.index = index;
        this.entityType = entityType;
    }

    @Override
    public Map<String,String> asMap()
    {
        Map<String,String> map = new HashMap<>();
        map.put( "indexType", "EXPLICIT INDEX" );
        map.put( "entityType", entityType );
        map.put( "identifier", identifier );
        map.put( "indexName", index );
        return map;
    }
}
