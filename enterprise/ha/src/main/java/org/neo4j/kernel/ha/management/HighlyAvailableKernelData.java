/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.management;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Functions;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.management.ClusterDatabaseInfo;
import org.neo4j.management.ClusterMemberInfo;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.Iterables.toArray;

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
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
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
        List<ClusterMemberInfo> clusterMemberInfos = new ArrayList<ClusterMemberInfo>(  );
        for ( ClusterMember clusterMember : memberInfo.getMembers() )
        {
            ClusterMemberInfo clusterMemberInfo = new ClusterMemberInfo( clusterMember.getInstanceId().toString(),
                    clusterMember.getHAUri() != null, clusterMember.isAlive(), clusterMember.getHARole(),
                    toArray( String.class, map( Functions.TO_STRING, clusterMember.getRoleURIs() ) ),
                    toArray( String.class, map( Functions.TO_STRING, clusterMember.getRoles() ) ) );
            clusterMemberInfos.add( clusterMemberInfo );
        }

        return clusterMemberInfos.toArray( new ClusterMemberInfo[clusterMemberInfos.size()] );
    }

    public ClusterDatabaseInfo getMemberInfo()
    {
        return memberInfoProvider.getInfo();
    }
}
