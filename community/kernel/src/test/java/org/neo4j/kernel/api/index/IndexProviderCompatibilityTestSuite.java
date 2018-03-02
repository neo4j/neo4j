/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.runner.ParameterizedSuiteRunner;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@RunWith( ParameterizedSuiteRunner.class )
@Suite.SuiteClasses( {
        SimpleIndexPopulatorCompatibility.General.class,
        SimpleIndexPopulatorCompatibility.Unique.class,
        CompositeIndexPopulatorCompatibility.General.class,
        CompositeIndexPopulatorCompatibility.Unique.class,
        SimpleIndexAccessorCompatibility.General.class,
        SimpleIndexAccessorCompatibility.Unique.class,
        CompositeIndexAccessorCompatibility.General.class,
        CompositeIndexAccessorCompatibility.Unique.class,
        UniqueConstraintCompatibility.class
} )
public abstract class IndexProviderCompatibilityTestSuite
{
    protected abstract IndexProvider createIndexProvider( PageCache pageCache, FileSystemAbstraction fs, File graphDbDir );

    public abstract Iterable<Value> getSupportedValues();

    protected List<Value> commonValues = Arrays.asList( Values.of( "string1" ), Values.of( 42 ) );
    protected List<Value> temporalValues = Arrays.asList( DateValue.epochDate( 2 ), DateValue.epochDate( 5 ) );
    protected List<Value> spatialValues = Arrays.asList(
            Values.pointValue( CoordinateReferenceSystem.Cartesian, 0, 0 ),
            Values.pointValue( CoordinateReferenceSystem.WGS84, 12.78, 56.7 ) );

    public abstract static class Compatibility
    {
        @Rule
        public final PageCacheAndDependenciesRule pageCacheAndDependenciesRule;

        protected File graphDbDir;
        protected FileSystemAbstraction fs;
        protected final IndexProviderCompatibilityTestSuite testSuite;
        protected IndexProvider indexProvider;
        protected SchemaIndexDescriptor descriptor;

        @Before
        public void setup()
        {
            fs = pageCacheAndDependenciesRule.fileSystem();
            graphDbDir = pageCacheAndDependenciesRule.directory().graphDbDir();
            PageCache pageCache = pageCacheAndDependenciesRule.pageCache();
            indexProvider = testSuite.createIndexProvider( pageCache, fs, graphDbDir );
        }

        public Compatibility( IndexProviderCompatibilityTestSuite testSuite, SchemaIndexDescriptor descriptor )
        {
            this.testSuite = testSuite;
            this.descriptor = descriptor;
            pageCacheAndDependenciesRule = new PageCacheAndDependenciesRule( DefaultFileSystemRule::new, testSuite.getClass() );
        }

        protected void withPopulator( IndexPopulator populator, ThrowingConsumer<IndexPopulator,Exception> runWithPopulator ) throws Exception
        {
            try
            {
                runWithPopulator.accept( populator );
            }
            finally
            {
                try
                {
                    populator.close( true );
                }
                catch ( Exception e )
                {   // ignore
                }
                try
                {
                    populator.close( false );
                }
                catch ( Exception e )
                {   // ignore
                }
            }
        }
    }
}
