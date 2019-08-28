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
package org.neo4j.kernel.database;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.configuration.helpers.NormalizedDatabaseName;

public class MapCachingDatabaseIdRepository implements DatabaseIdRepository.Caching
{
    private static final Optional<DatabaseId> OPT_SYS_DB = Optional.of( SYSTEM_DATABASE_ID );

    private final DatabaseIdRepository delegate;
    private final Map<String,DatabaseId> databaseIdsByName;
    private final Map<UUID,DatabaseId> databaseIdsByUuid;

    public MapCachingDatabaseIdRepository( DatabaseIdRepository delegate )
    {
        this.delegate = delegate;
        this.databaseIdsByName = new ConcurrentHashMap<>();
        this.databaseIdsByUuid = new ConcurrentHashMap<>();
    }

    @Override
    public Optional<DatabaseId> getByName( NormalizedDatabaseName databaseName )
    {
        if ( SYSTEM_DATABASE_ID.name().equals( databaseName.name() ) )
        {
            return OPT_SYS_DB;
        }
        return Optional.ofNullable(
                databaseIdsByName.computeIfAbsent( databaseName.name(), name -> delegate.getByName( name ).orElse( null ) )
        );
    }

    @Override
    public Optional<DatabaseId> getByUuid( UUID uuid )
    {
        if ( SYSTEM_DATABASE_ID.uuid().equals( uuid ) )
        {
            return OPT_SYS_DB;
        }
        return Optional.ofNullable(
                databaseIdsByUuid.computeIfAbsent( uuid, id -> delegate.getByUuid( id ).orElse( null ) )
        );
    }

    @Override
    public void invalidate( DatabaseId databaseId )
    {
        databaseIdsByName.remove( databaseId.name() );
        databaseIdsByUuid.remove( databaseId.uuid() );
    }

    @Override
    public void cache( DatabaseId databaseId )
    {
        this.databaseIdsByName.put( databaseId.name(), databaseId );
        this.databaseIdsByUuid.put( databaseId.uuid(), databaseId );
    }
}
