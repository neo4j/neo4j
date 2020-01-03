/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.database;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.configuration.helpers.NormalizedDatabaseName;

public class MapCachingDatabaseIdRepository implements DatabaseIdRepository.Caching
{
    private static final Optional<NamedDatabaseId> OPT_SYS_DB = Optional.of( NAMED_SYSTEM_DATABASE_ID );

    private final DatabaseIdRepository delegate;
    private final Map<String,NamedDatabaseId> databaseIdsByName;
    private final Map<DatabaseId,NamedDatabaseId> databaseIdsByUuid;

    public MapCachingDatabaseIdRepository( DatabaseIdRepository delegate )
    {
        this.delegate = delegate;
        this.databaseIdsByName = new ConcurrentHashMap<>();
        this.databaseIdsByUuid = new ConcurrentHashMap<>();
    }

    @Override
    public Optional<NamedDatabaseId> getByName( NormalizedDatabaseName databaseName )
    {
        if ( NAMED_SYSTEM_DATABASE_ID.name().equals( databaseName.name() ) )
        {
            return OPT_SYS_DB;
        }
        return Optional.ofNullable(
                databaseIdsByName.computeIfAbsent( databaseName.name(), name -> delegate.getByName( name ).orElse( null ) )
        );
    }

    @Override
    public Optional<NamedDatabaseId> getById( DatabaseId uuid )
    {
        if ( NAMED_SYSTEM_DATABASE_ID.databaseId().equals( uuid ) )
        {
            return OPT_SYS_DB;
        }
        return Optional.ofNullable(
                databaseIdsByUuid.computeIfAbsent( uuid, id -> delegate.getById( id ).orElse( null ) )
        );
    }

    @Override
    public void invalidate( NamedDatabaseId namedDatabaseId )
    {
        databaseIdsByName.remove( namedDatabaseId.name() );
        databaseIdsByUuid.remove( namedDatabaseId.databaseId() );
    }

    @Override
    public void cache( NamedDatabaseId namedDatabaseId )
    {
        this.databaseIdsByName.put( namedDatabaseId.name(), namedDatabaseId );
        this.databaseIdsByUuid.put( namedDatabaseId.databaseId(), namedDatabaseId );
    }
}
