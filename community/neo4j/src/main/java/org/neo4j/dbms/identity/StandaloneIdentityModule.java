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
package org.neo4j.dbms.identity;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

public class StandaloneIdentityModule implements IdentityModule
{
    public static IdentityModule create( LogProvider logProvider, FileSystemAbstraction fs, File dataDir, MemoryTracker memoryTracker )
    {
       return new StandaloneIdentityModule( logProvider, createServerIdStorage( fs, dataDir, memoryTracker ), ServerId::new, UUID::randomUUID );
    }

    private final ServerId myself;

    protected StandaloneIdentityModule( LogProvider logProvider, SimpleStorage<ServerId> storage, Function<UUID,ServerId> creator, Supplier<UUID> uuid )
    {
        myself = readOrGenerate( storage, logProvider.getLog( getClass() ), ServerId.class.getSimpleName(), creator, uuid );
    }

    @Override
    public ServerId myself()
    {
        return myself;
    }

    protected static <T> T readOrGenerate( SimpleStorage<T> storage, Log log, String idType, Function<UUID, T> creator, Supplier<UUID> uuid )
    {
        T myself;
        try
        {
            if ( storage.exists() )
            {
                myself = storage.readState();
                if ( myself == null )
                {
                    throw new IllegalStateException( String.format( "%s storage was found on disk, but it could not be read correctly", idType ) );
                }
                else
                {
                    log.info( String.format( "Found %s on disk: %s", idType, myself ) );
                }
            }
            else
            {
                UUID newUuid = uuid.get();
                myself = creator.apply( newUuid );
                storage.writeState( myself );

                log.info( String.format( "Generated new %s: %s (%s)", idType, myself, newUuid ) );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return myself;
    }

    protected static SimpleStorage<ServerId> createServerIdStorage( FileSystemAbstraction fs, File dataDir, MemoryTracker memoryTracker )
    {
        return new SimpleFileStorage<>( fs, new File( dataDir, SERVER_ID_FILENAME ), ServerId.Marshal.INSTANCE, memoryTracker );
    }
}
