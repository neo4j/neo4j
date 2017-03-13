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
package org.neo4j.kernel.api.query;

import java.util.Map;

public abstract class IndexUsage
{
    public static IndexUsage schemaIndexUsage( String identifier, String label, String... propertyKeys )
    {
        return new SchemaIndexUsage( identifier, label, propertyKeys );
    }

    public static IndexUsage legacyIndexUsage( String identifier, String entityType, String index )
    {
        return new LegacyIndexUsage( identifier, index, entityType );
    }

    public abstract Map<String,String> asMap();

    final String identifier;

    IndexUsage( String identifier )
    {
        this.identifier = identifier;
    }
}
