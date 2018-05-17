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
package org.neo4j.causalclustering.stresstests;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.neo4j.causalclustering.stresstests.ClusterStressTesting.stressTest;

public class ClusterStressScenarioSmoke
{
    private final DefaultFileSystemRule fileSystem = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystem ).around( pageCacheRule );

    private Config config;
    private PageCache pageCache;

    @Before
    public void setup()
    {
        this.pageCache = pageCacheRule.getPageCache( fileSystem );

        config = new Config();
        config.workDurationMinutes( 1 );
    }

    @Test
    public void stressBackupRandomMemberAndStartStop() throws Exception
    {
        config.workloads( Workloads.CreateNodesWithProperties, Workloads.BackupRandomMember, Workloads.StartStopRandomMember );
        stressTest( config, fileSystem, pageCache );
    }

    @Test
    public void stressCatchupNewReadReplica() throws Exception
    {
        config.workloads( Workloads.CreateNodesWithProperties, Workloads.CatchupNewReadReplica, Workloads.StartStopRandomCore );
        stressTest( config, fileSystem, pageCache );
    }

    @Test
    public void stressReplaceRandomMember() throws Exception
    {
        config.workloads( Workloads.CreateNodesWithProperties, Workloads.ReplaceRandomMember );
        stressTest( config, fileSystem, pageCache );
    }

    @Test
    public void stressIdReuse() throws Exception
    {
        config.numberOfEdges( 0 );
        config.reelectIntervalSeconds( 20 );

        config.preparations( Preparations.IdReuseSetup );

        // having two deletion workers is on purpose
        config.workloads( Workloads.IdReuseInsertion, Workloads.IdReuseDeletion, Workloads.IdReuseDeletion, Workloads.IdReuseReelection );
        config.validations( Validations.ConsistencyCheck, Validations.IdReuseUniqueFreeIds );

        stressTest( config, fileSystem, pageCache );
    }
}
