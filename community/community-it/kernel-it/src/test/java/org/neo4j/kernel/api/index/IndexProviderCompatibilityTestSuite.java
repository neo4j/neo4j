/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.schema.SchemaTestUtil;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.runner.ParameterizedSuiteRunner;

import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@RunWith( ParameterizedSuiteRunner.class )
@Suite.SuiteClasses( {
        IndexConfigurationCompletionCompatibility.class,
        MinimalIndexAccessorCompatibility.General.class,
        MinimalIndexAccessorCompatibility.ReadOnly.class
} )
public abstract class IndexProviderCompatibilityTestSuite
{
    protected abstract IndexProvider createIndexProvider( PageCache pageCache, FileSystemAbstraction fs, Path graphDbDir,
            Config config );

    protected abstract IndexPrototype indexPrototype();

    public void consistencyCheck( IndexPopulator populator )
    {
        // no-op by default
    }

    public void additionalConfig( Config.Builder configBuilder )
    {
        //can be overridden in sub-classes that wants to add additional Config settings.
    }

    public abstract static class Compatibility
    {
        private final PageCacheAndDependenciesRule pageCacheAndDependenciesRule;
        final RandomRule random;

        @Rule
        public RuleChain ruleChain;

        Path graphDbDir;
        protected FileSystemAbstraction fs;
        protected IndexProvider indexProvider;
        private final IndexPrototype incompleteIndexPrototype;
        protected IndexDescriptor descriptor;
        protected TokenNameLookup tokenNameLookup;
        final IndexProviderCompatibilityTestSuite testSuite;
        final JobScheduler jobScheduler;
        final IndexPopulator.PopulationWorkScheduler populationWorkScheduler;
        private final Config config;

        @Before
        public void setup() throws Exception
        {
            fs = pageCacheAndDependenciesRule.fileSystem();
            graphDbDir = pageCacheAndDependenciesRule.directory().homePath();
            PageCache pageCache = pageCacheAndDependenciesRule.pageCache();
            indexProvider = testSuite.createIndexProvider( pageCache, fs, graphDbDir, config );
            descriptor = indexProvider.completeConfiguration( incompleteIndexPrototype.withName( "index_17" ).materialise( 17 ) );
            jobScheduler.start();
        }

        @After
        public void tearDown() throws Exception
        {
            if ( jobScheduler != null )
            {
                jobScheduler.shutdown();
            }
        }

        public void additionalConfig( Config.Builder configBuilder )
        {
            //can be overridden in sub-classes that wants to add additional Config settings.
        }

        Compatibility( IndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype )
        {
            this.testSuite = testSuite;

            Config.Builder configBuilder = Config.newBuilder();
            testSuite.additionalConfig( configBuilder );
            additionalConfig( configBuilder );
            this.config = configBuilder.build();

            this.incompleteIndexPrototype = prototype;
            jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
            populationWorkScheduler = new IndexPopulator.PopulationWorkScheduler()
            {

                @Override
                public <T> JobHandle<T> schedule( IndexPopulator.JobDescriptionSupplier descriptionSupplier, Callable<T> job )
                {
                    return jobScheduler.schedule( Group.INDEX_POPULATION_WORK, new JobMonitoringParams( null, null, null ), job );
                }
            };
            pageCacheAndDependenciesRule = new PageCacheAndDependenciesRule().with( new DefaultFileSystemRule() ).with( testSuite.getClass() );
            random = new RandomRule();
            ruleChain = RuleChain.outerRule( pageCacheAndDependenciesRule ).around( random );
            tokenNameLookup = SchemaTestUtil.SIMPLE_NAME_LOOKUP;
        }

        void withPopulator( IndexPopulator populator, ThrowingConsumer<IndexPopulator,Exception> runWithPopulator ) throws Exception
        {
            withPopulator( populator, runWithPopulator, true );
        }

        void withPopulator( IndexPopulator populator, ThrowingConsumer<IndexPopulator,Exception> runWithPopulator, boolean closeSuccessfully ) throws Exception
        {
            try
            {
                populator.create();
                runWithPopulator.accept( populator );
                if ( closeSuccessfully )
                {
                    populator.scanCompleted( PhaseTracker.nullInstance, populationWorkScheduler, NULL );
                    testSuite.consistencyCheck( populator );
                }
            }
            finally
            {
                populator.close( closeSuccessfully, NULL );
            }
        }
    }
}
