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
package org.neo4j.kernel.ha.management;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.management.ClusterDatabaseInfo;
import org.neo4j.management.ClusterMemberInfo;

import static org.neo4j.helpers.collection.Iterables.asArray;
import static org.neo4j.helpers.collection.Iterables.map;

public class HighlyAvailableKernelData extends KernelData implements Lifecycle
{
    private final GraphDatabaseAPI db;
    private final ClusterMembers memberInfo;
    private final ClusterDatabaseInfoProvider memberInfoProvider;

    public HighlyAvailableKernelData( GraphDatabaseAPI db, ClusterMembers memberInfo,
            ClusterDatabaseInfoProvider databaseInfo, FileSystemAbstraction fileSystem, PageCache pageCache,
            File storeDir, Config config )
    {
        super( fileSystem, pageCache, storeDir, config );
        this.db = db;
        this.memberInfo = memberInfo;
        this.memberInfoProvider = databaseInfo;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
    }

    @Override
    public Version version()
    {
        return Version.getKernel();
    }

    @Override
    public GraphDatabaseAPI graphDatabase()
    {
        return db;
    }

    public ClusterMemberInfo[] getClusterInfo()
    {
        List<ClusterMemberInfo> clusterMemberInfos = new ArrayList<>();
        Function<Object,String> nullSafeToString = from -> from == null ? "" : from.toString();
        for ( ClusterMember clusterMember : memberInfo.getMembers() )
        {
            ClusterMemberInfo clusterMemberInfo = new ClusterMemberInfo( clusterMember.getInstanceId().toString(),
                    clusterMember.getHAUri() != null, clusterMember.isAlive(), clusterMember.getHARole(),
                    asArray( String.class, map( nullSafeToString, clusterMember.getRoleURIs() ) ),
                    asArray( String.class, map( nullSafeToString, clusterMember.getRoles() ) ) );
            clusterMemberInfos.add( clusterMemberInfo );
        }

        return clusterMemberInfos.toArray( new ClusterMemberInfo[clusterMemberInfos.size()] );
    }

    public ClusterDatabaseInfo getMemberInfo()
    {
        return memberInfoProvider.getInfo();
    }
}
