/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.causalclustering.core.state.ClusterStateCleaner;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class MemberIdRepository extends LifecycleAdapter
{
    public static final String CORE_MEMBER_ID_NAME = "core-member-id";

    private final MemberId myself;
    private final SimpleStorage<MemberId> memberIdStorage;
    private final boolean replaceExistingState;

    public MemberIdRepository( PlatformModule platformModule, ClusterStateDirectory clusterStateDirectory, ClusterStateCleaner clusterStateCleaner )
    {
        FileSystemAbstraction fileSystem = platformModule.fileSystem;
        LogProvider logProvider = platformModule.logging.getInternalLogProvider();

        Log log = logProvider.getLog( getClass() );

        memberIdStorage = new SimpleFileStorage<>( fileSystem, clusterStateDirectory.get(), CORE_MEMBER_ID_NAME, new MemberId.Marshal(), logProvider );

        try
        {
            if ( memberIdStorage.exists() && !clusterStateCleaner.stateUnclean() )
            {
                myself = memberIdStorage.readState();
                replaceExistingState = false;
                if ( myself == null )
                {
                    throw new RuntimeException( "MemberId stored on disk was null" );
                }
            }
            else
            {
                UUID uuid = UUID.randomUUID();
                myself = new MemberId( uuid );
                replaceExistingState = true;
                log.info( String.format( "Generated new id: %s (%s)", myself, uuid ) );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        platformModule.jobScheduler.setTopLevelGroupName( "Core " + myself );
    }

    public MemberId myself()
    {
        return myself;
    }

    @Override
    public void init() throws Throwable
    {
        if ( replaceExistingState )
        {
            memberIdStorage.writeState( myself );
        }
    }
}
