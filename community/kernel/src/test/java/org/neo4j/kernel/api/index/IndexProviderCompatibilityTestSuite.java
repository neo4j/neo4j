/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.File;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
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

    public abstract boolean supportsSpatial();

    public abstract boolean supportsTemporal();

    public abstract static class Compatibility
    {
        @Rule
        public final PageCacheAndDependenciesRule pageCacheAndDependenciesRule;

        protected File graphDbDir;
        protected FileSystemAbstraction fs;
        protected IndexProvider indexProvider;
        protected SchemaIndexDescriptor descriptor;
        final IndexProviderCompatibilityTestSuite testSuite;
        final List<NodeAndValue> valueSet1;
        final List<NodeAndValue> valueSet2;

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
            this.valueSet1 = allValues(
                    testSuite.supportsSpatial(),
                    testSuite.supportsTemporal(),
                    Arrays.asList( Values.of( "string1" ), Values.of( 42 ) ),
                    Arrays.asList(
                            DateValue.epochDate( 2 ),
                            LocalTimeValue.localTime( 100000 ),
                            TimeValue.time( 43_200_000_000_000L, ZoneOffset.UTC ), // Noon
                            TimeValue.time( 43_201_000_000_000L, ZoneOffset.UTC ),
                            TimeValue.time( 43_200_000_000_000L, ZoneOffset.of( "+01:00" ) ), // Noon in the next time-zone
                            TimeValue.time( 46_800_000_000_000L, ZoneOffset.UTC ), // Same time UTC as prev time
                            LocalDateTimeValue.localDateTime( 2018, 3, 1, 13, 50, 42, 1337 ),
                            DateTimeValue.datetime( 2014, 3, 25, 12, 45, 13, 7474, "UTC" ),
                            DateTimeValue.datetime( 2014, 3, 25, 12, 45, 13, 7474, "Europe/Stockholm" ),
                            DateTimeValue.datetime( 2014, 3, 25, 12, 45, 13, 7474, "+05:00" ),
                            DateTimeValue.datetime( 2015, 3, 25, 12, 45, 13, 7474, "+05:00" ),
                            DateTimeValue.datetime( 2014, 4, 25, 12, 45, 13, 7474, "+05:00" ),
                            DateTimeValue.datetime( 2014, 3, 26, 12, 45, 13, 7474, "+05:00" ),
                            DateTimeValue.datetime( 2014, 3, 25, 13, 45, 13, 7474, "+05:00" ),
                            DateTimeValue.datetime( 2014, 3, 25, 12, 46, 13, 7474, "+05:00" ),
                            DateTimeValue.datetime( 2014, 3, 25, 12, 45, 14, 7474, "+05:00" ),
                            DateTimeValue.datetime( 2014, 3, 25, 12, 45, 14, 7475, "+05:00" ),
                            // only runnable it JVM supports East-Saskatchewan
                            // DateTimeValue.datetime( 2001, 1, 25, 11, 11, 30, 0, "Canada/East-Saskatchewan" ),
                            DateTimeValue.datetime( 2038, 1, 18, 9, 14, 7, 0, "-18:00" ),
                            DateTimeValue.datetime( 10000, 100, ZoneOffset.ofTotalSeconds( 3 ) ),
                            DateTimeValue.datetime( 10000, 101, ZoneOffset.ofTotalSeconds( -3 ) ),
                            DurationValue.duration( 10, 20, 30, 40 ),
                            DurationValue.duration( 11, 20, 30, 40 ),
                            DurationValue.duration( 10, 21, 30, 40 ),
                            DurationValue.duration( 10, 20, 31, 40 ),
                            DurationValue.duration( 10, 20, 30, 41 ) ),
                    Arrays.asList( Values.pointValue( CoordinateReferenceSystem.Cartesian, 0, 0 ),
                                   Values.pointValue( CoordinateReferenceSystem.WGS84, 12.78, 56.7 ) ) );

            this.valueSet2 = allValues(
                    testSuite.supportsSpatial(),
                    testSuite.supportsTemporal(),
                    Arrays.asList( Values.of( "string2" ), Values.of( 1337 ) ),
                    Arrays.asList(
                            DateValue.epochDate( 42 ),
                            LocalTimeValue.localTime( 2000 ),
                            TimeValue.time( 100L, ZoneOffset.UTC ), // Just around midnight
                            LocalDateTimeValue.localDateTime( 2018, 2, 28, 11, 5, 1, 42 ),
                            DateTimeValue.datetime( 1999, 12, 31, 23, 59, 59, 123456789, "Europe/London" ),
                            DurationValue.duration( 4, 3, 2, 1 ) ),
                    Arrays.asList( Values.pointValue( CoordinateReferenceSystem.Cartesian, 10, 10 ),
                            Values.pointValue( CoordinateReferenceSystem.WGS84, 87.21, 7.65 ) ) );

            pageCacheAndDependenciesRule = new PageCacheAndDependenciesRule( DefaultFileSystemRule::new, testSuite.getClass() );
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
            }
            finally
            {
                populator.close( closeSuccessfully );
            }
        }

        List<IndexEntryUpdate<?>> updates( List<NodeAndValue> values )
        {
            return updates( values, 0 );
        }

        List<IndexEntryUpdate<?>> updates( List<NodeAndValue> values, long nodeIdOffset )
        {
            List<IndexEntryUpdate<?>> updates = new ArrayList<>();
            values.forEach( entry -> updates.add( IndexEntryUpdate.add( nodeIdOffset + entry.nodeId, descriptor.schema(), entry.value ) ) );
            return updates;
        }

        private static List<NodeAndValue> allValues( boolean supportsSpatial,
                                                     boolean supportsTemporal,
                                                     List<Value> common,
                                                     List<Value> temporal,
                                                     List<Value> spatial )
        {
            long nodeIds = 0;
            List<NodeAndValue> result = new ArrayList<>();
            for ( Value value : common )
            {
                result.add( new NodeAndValue( nodeIds++, value ) );
            }
            if ( supportsSpatial )
            {
                for ( Value value : spatial )
                {
                    result.add( new NodeAndValue( nodeIds++, value ) );
                }
            }
            if ( supportsTemporal )
            {
                for ( Value value : temporal )
                {
                    result.add( new NodeAndValue( nodeIds++, value ) );
                }
            }
            return result;
        }

        static class NodeAndValue
        {
            final long nodeId;
            final Value value;

            NodeAndValue( long nodeId, Value value )
            {
                this.nodeId = nodeId;
                this.value = value;
            }
        }
    }
}
