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

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.logging.Log;
import org.neo4j.memory.MemoryTracker;

public abstract class DefaultIdentityModule implements IdentityModule
{
    protected static <T extends ServerId> T readOrGenerate( SimpleStorage<T> storage, Log log, Class<T> type, Function<UUID,T> creator, Supplier<UUID> uuid )
    {
        T myself;
        try
        {
            if ( storage.exists() )
            {
                myself = storage.readState();
                if ( myself == null )
                {
                    throw new IllegalStateException(
                            String.format( "%s storage was found on disk, but it could not be read correctly", type.getSimpleName() ) );
                }
                else
                {
                    log.info( String.format( "Found %s on disk: %s (%s)", type.getSimpleName(), myself, myself.getUuid() ) );
                }
            }
            else
            {
                UUID newUuid = uuid.get();
                myself = creator.apply( newUuid );
                storage.writeState( myself );

                log.info( String.format( "Generated new %s: %s (%s)", type.getSimpleName(), myself, newUuid ) );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return myself;
    }

    protected static SimpleStorage<ServerId> createServerIdStorage( FileSystemAbstraction fs, Path serverIdFile, MemoryTracker memoryTracker )
    {
        return new SimpleFileStorage<>( fs, serverIdFile.toFile(), ServerId.Marshal.INSTANCE, memoryTracker );
    }
}
