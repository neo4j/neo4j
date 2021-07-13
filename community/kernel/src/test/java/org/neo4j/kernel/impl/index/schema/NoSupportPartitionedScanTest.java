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

import org.junit.jupiter.api.Nested;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexProvider;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

import static org.mockito.Mockito.mock;
import static org.neo4j.internal.schema.IndexCapability.NO_CAPABILITY;

public class NoSupportPartitionedScanTest extends SupportPartitionedScanTestSuite
{
    NoSupportPartitionedScanTest()
    {
        super( NO_CAPABILITY, NO_SUPPORT );
    }

    // range index being implemented
    @Nested
    class Range extends SupportPartitionedScanTestSuite
    {
        Range()
        {
            super( RangeIndexReader.CAPABILITY, NO_SUPPORT );
        }
    }

    // point index being implemented
    @Nested
    class Point extends SupportPartitionedScanTestSuite
    {
        // capability has yet to be implemented
        Point()
        {
            super( NO_CAPABILITY, NO_SUPPORT );
        }
    }

    // text index being implemented
    @Nested
    class Text extends SupportPartitionedScanTestSuite
    {
        Text()
        {
            super( TextIndexProvider.CAPABILITY, NO_SUPPORT );
        }
    }

    @Nested
    class Fulltext extends SupportPartitionedScanTestSuite
    {
        Fulltext()
        {
            super( fusionCapability(), NO_SUPPORT );
        }
    }

    private static IndexCapability fusionCapability()
    {
        // require the IndexProvider to complete the configuration of the IndexDescriptor
        // such that the IndexCapability is correctly set, and not just IndexCapability.NO_CAPABILITY
        // mock unimportant things to the test
        final var descriptor = FulltextIndexProviderFactory.DESCRIPTOR;
        final var provider = new FulltextIndexProvider( descriptor,
                                                        mock( IndexDirectoryStructure.Factory.class ),
                                                        mock( FileSystemAbstraction.class ),
                                                        Config.defaults(),
                                                        mock( TokenHolders.class ),
                                                        mock( DirectoryFactory.class ),
                                                        mock( DatabaseReadOnlyChecker.class ),
                                                        mock( JobScheduler.class ),
                                                        mock( Log.class ) );

        final var ids = idGenerator();
        final var index = provider.completeConfiguration(
                IndexPrototype.forSchema( SchemaDescriptors.forLabel( ids.getAsInt(), ids.getAsInt(), ids.getAsInt() ) )
                              .withName( "Fulltext" )
                              .withIndexProvider( descriptor )
                              .materialise( ids.getAsInt() ) );

        return index.getCapability();
    }
}
