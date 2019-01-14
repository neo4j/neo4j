/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
