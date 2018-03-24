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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.helper.Workload;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.Log;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

public class ClusterStressTesting
{
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule );

    private FileSystemAbstraction fileSystem;
    private PageCache pageCache;

    @Before
    public void setup()
    {
        fileSystem = fileSystemRule.get();
        pageCache = pageCacheRule.getPageCache( fileSystem );
    }

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        stressTest( new Config(), fileSystem, pageCache );
    }

    static void stressTest( Config config, FileSystemAbstraction fileSystem, PageCache pageCache ) throws Exception
    {
        Resources resources = new Resources( fileSystem, pageCache, config );
        Control control = new Control( config );
        Log log = config.logProvider().getLog( ClusterStressTesting.class );

        log.info( config.toString() );

        List<Preparation> preparations = config.preparations().stream()
                .map( preparation -> preparation.create( resources ) )
                .collect( Collectors.toList() );

        List<Workload> workloads = config.workloads().stream()
                .map( workload -> workload.create( control, resources, config ) )
                .collect( Collectors.toList() );

        List<Validation> validations = config.validations().stream()
                .map( validator -> validator.create( resources ) )
                .collect( Collectors.toList() );

        if ( workloads.size() == 0 )
        {
            throw new IllegalArgumentException( "No workloads." );
        }

        ExecutorService executor = Executors.newCachedThreadPool();

        try
        {
            log.info( "Starting resources" );
            resources.start();

            log.info( "Preparing scenario" );
            for ( Preparation preparation : preparations )
            {
                preparation.prepare();
            }

            log.info( "Preparing workloads" );
            for ( Workload workload : workloads )
            {
                workload.prepare();
            }

            log.info( "Starting workloads" );
            List<Future<?>> completions = new ArrayList<>();
            for ( Workload workload : workloads )
            {
                completions.add( executor.submit( workload ) );
            }

            control.awaitEnd( completions );

            for ( Workload workload : workloads )
            {
                workload.validate();
            }
        }
        catch ( Throwable cause )
        {
            control.onFailure( cause );
        }

        log.info( "Shutting down executor" );
        executor.shutdownNow();
        executor.awaitTermination( 5, TimeUnit.MINUTES );

        log.info( "Stopping resources" );
        resources.stop();

        control.assertNoFailure();

        log.info( "Validating results" );
        for ( Validation validation : validations )
        {
            validation.validate();
        }

        // let us only cleanup resources when everything went well, and otherwise leave them for post-mortem
        log.info( "Cleaning up" );
        resources.cleanup();
    }
}
