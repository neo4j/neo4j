/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cluster.protocol.cluster;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * TODO
 */
@Ignore
@RunWith(Parameterized.class)
public class ClusterRandomTest
        extends ClusterMockTest
{
    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        return seeds( 1338272692543769000L, 1338272692343957000L, 1338272692188718000L, 1338272692117545000L,
                1338272692020413000L, 1338272691938947000L, 1338272691895131000L, 1338272691832332000L,
                1338272691540039000L,
                1338272632660010000L, 1337830212532839000L );

//        return seeds( 1349765117306363000L );
        //       return seeds( -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 );
    }

    private static Collection<Object[]> seeds( long... s )
    {
        List<Object[]> seedList = new ArrayList<Object[]>();
        for ( long seed : s )
        {
            seedList.add( new Object[]{seed} );
        }
        return seedList;
    }

    final long seed;

    public ClusterRandomTest( long s )
    {
        seed = s;
    }

    @Test
    public void randomTest()
            throws URISyntaxException, ExecutionException, TimeoutException, InterruptedException
    {
        testCluster( 7, DEFAULT_NETWORK(), new ClusterTestScriptRandom( seed ) );
    }
}
