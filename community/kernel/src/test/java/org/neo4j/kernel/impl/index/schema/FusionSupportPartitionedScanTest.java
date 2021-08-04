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
package org.neo4j.kernel.impl.index.schema;

import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30;
import org.neo4j.monitoring.Monitors;

import static org.mockito.Mockito.mock;
import static org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension.TEST_DIRECTORY;

public class FusionSupportPartitionedScanTest extends SupportPartitionedScanTestSuite
{
    public FusionSupportPartitionedScanTest()
    {
        super( capability(), supports(
                Query.EXISTS,

                Query.EXACT_NUMBER,
                Query.EXACT_NUMBER_ARRAY,
                Query.EXACT_TEXT_ARRAY,
                Query.EXACT_GEOMETRY,
                Query.EXACT_GEOMETRY_ARRAY,
                Query.EXACT_TEMPORAL,
                Query.EXACT_TEMPORAL_ARRAY,
                Query.EXACT_BOOLEAN,
                Query.EXACT_BOOLEAN_ARRAY,

                Query.RANGE_NUMBER,
                Query.RANGE_NUMBER_ARRAY,
                Query.RANGE_TEXT_ARRAY,
                Query.RANGE_TEMPORAL,
                Query.RANGE_TEMPORAL_ARRAY,
                Query.RANGE_BOOLEAN,
                Query.RANGE_BOOLEAN_ARRAY,

                Query.COMPOSITE_EXISTS_EXISTS,

                Query.COMPOSITE_EXACT_NUMBER_EXISTS,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_TEXT,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_NUMBER_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_NUMBER_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_RANGE_TEXT,
                Query.COMPOSITE_EXACT_NUMBER_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_NUMBER_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_NUMBER_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_STRING_PREFIX,

                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXISTS,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_TEXT,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_RANGE_TEXT,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_NUMBER_ARRAY_STRING_PREFIX,

                Query.COMPOSITE_EXACT_TEXT_EXISTS,
                Query.COMPOSITE_EXACT_TEXT_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_TEXT_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_EXACT_TEXT,
                Query.COMPOSITE_EXACT_TEXT_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_TEXT_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_TEXT_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_TEXT_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_TEXT_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_RANGE_TEXT,
                Query.COMPOSITE_EXACT_TEXT_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_TEXT_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_TEXT_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_STRING_PREFIX,

                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXISTS,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_TEXT,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_RANGE_TEXT,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_TEXT_ARRAY_STRING_PREFIX,

                Query.COMPOSITE_EXACT_GEOMETRY_EXISTS,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_TEXT,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_GEOMETRY_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_GEOMETRY_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_RANGE_TEXT,
                Query.COMPOSITE_EXACT_GEOMETRY_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_GEOMETRY_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_GEOMETRY_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_STRING_PREFIX,

                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXISTS,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_TEXT,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_RANGE_TEXT,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_GEOMETRY_ARRAY_STRING_PREFIX,

                Query.COMPOSITE_EXACT_TEMPORAL_EXISTS,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_TEXT,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_TEMPORAL_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_TEMPORAL_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_RANGE_TEXT,
                Query.COMPOSITE_EXACT_TEMPORAL_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_TEMPORAL_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_TEMPORAL_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_STRING_PREFIX,

                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXISTS,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_TEXT,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_RANGE_TEXT,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_TEMPORAL_ARRAY_STRING_PREFIX,

                Query.COMPOSITE_EXACT_BOOLEAN_EXISTS,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_TEXT,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_BOOLEAN_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_BOOLEAN_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_RANGE_TEXT,
                Query.COMPOSITE_EXACT_BOOLEAN_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_BOOLEAN_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_BOOLEAN_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_STRING_PREFIX,

                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXISTS,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_NUMBER,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_TEXT,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_GEOMETRY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_GEOMETRY_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_TEMPORAL,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_BOOLEAN,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_EXACT_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_RANGE_NUMBER,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_RANGE_NUMBER_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_RANGE_TEXT,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_RANGE_TEXT_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_RANGE_TEMPORAL,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_RANGE_TEMPORAL_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_RANGE_BOOLEAN,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_RANGE_BOOLEAN_ARRAY,
                Query.COMPOSITE_EXACT_BOOLEAN_ARRAY_STRING_PREFIX,

                Query.COMPOSITE_RANGE_NUMBER_EXISTS,
                Query.COMPOSITE_RANGE_NUMBER_ARRAY_EXISTS,
                Query.COMPOSITE_RANGE_TEXT_EXISTS,
                Query.COMPOSITE_RANGE_TEXT_ARRAY_EXISTS,
                Query.COMPOSITE_RANGE_TEMPORAL_EXISTS,
                Query.COMPOSITE_RANGE_TEMPORAL_ARRAY_EXISTS,
                Query.COMPOSITE_RANGE_BOOLEAN_EXISTS,
                Query.COMPOSITE_RANGE_BOOLEAN_ARRAY_EXISTS,

                Query.COMPOSITE_STRING_PREFIX_EXISTS ) );
    }

    private static IndexCapability capability()
    {
        // require the IndexProvider to complete the configuration of the IndexDescriptor
        // such that the IndexCapability is correctly set, and not just IndexCapability.NO_CAPABILITY
        // mock unimportant things to the test
        final var provider = NativeLuceneFusionIndexProviderFactory30.create( mock( PageCache.class ),
                                                                              Path.of( TEST_DIRECTORY ),
                                                                              mock( FileSystemAbstraction.class ),
                                                                              mock( Monitors.class ),
                                                                              "testTag",
                                                                              Config.defaults(),
                                                                              mock( DatabaseReadOnlyChecker.class ),
                                                                              mock( RecoveryCleanupWorkCollector.class ),
                                                                              mock( PageCacheTracer.class ),
                                                                              "testDatabase" );

        final var ids = idGenerator();
        final var index = provider.completeConfiguration(
                IndexPrototype.forSchema( SchemaDescriptors.forLabel( ids.getAsInt(), ids.getAsInt(), ids.getAsInt() ) )
                              .withName( "Fusion" )
                              .withIndexProvider( NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR )
                              .materialise( ids.getAsInt() ) );

        return index.getCapability();
    }
}
