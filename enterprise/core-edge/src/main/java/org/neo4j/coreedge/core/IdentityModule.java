/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.core;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.neo4j.coreedge.core.state.storage.SimpleStorage;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class IdentityModule
{
    public static final String CORE_MEMBER_ID_NAME = "core-member-id";
    private MemberId myself;

    IdentityModule( PlatformModule platformModule, File clusterStateDirectory )
    {
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        LogProvider logProvider = platformModule.logging.getInternalLogProvider();

        Log log = logProvider.getLog( getClass() );

        SimpleStorage<MemberId> memberIdStorage = new SimpleStorage<>( fileSystem, clusterStateDirectory,
                CORE_MEMBER_ID_NAME, new MemberId.Marshal(), logProvider );

        try
        {
            if ( memberIdStorage.exists() )
            {
                myself = memberIdStorage.readState();
                if ( myself == null )
                {
                    throw new RuntimeException( "I was null" );
                }
            }
            else
            {
                UUID uuid = UUID.randomUUID();
                myself = new MemberId( uuid );
                memberIdStorage.writeState( myself );

                log.info( String.format( "Generated new id: %s (%s)", myself, uuid ) );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    MemberId myself()
    {
        return myself;
    }
}
