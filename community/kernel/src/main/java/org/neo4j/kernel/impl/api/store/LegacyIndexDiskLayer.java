/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.kernel.api.LegacyIndexHits;

public class LegacyIndexDiskLayer implements LegacyIndexReadLayer
{
    @Override
    public LegacyIndexHits nodeIndexGet( String indexName, String key, Object value )
    {
        return null;
    }

    @Override
    public LegacyIndexHits nodeIndexQuery( String indexName, String key, Object queryOrQueryObject )
    {
        return null;
    }

    @Override
    public LegacyIndexHits nodeIndexQuery( String indexName, Object queryOrQueryObject )
    {
        return null;
    }

    @Override
    public LegacyIndexHits relationshipIndexGet( String indexName, String key, Object value )
    {
        return null;
    }

    @Override
    public LegacyIndexHits relationshipIndexQuery( String indexName, String key, Object queryOrQueryObject )
    {
        return null;
    }

    @Override
    public LegacyIndexHits relationshipIndexQuery( String indexName, Object queryOrQueryObject )
    {
        return null;
    }
}
