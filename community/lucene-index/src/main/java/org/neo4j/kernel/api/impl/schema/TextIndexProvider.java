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
package org.neo4j.kernel.api.impl.schema;

import java.util.Arrays;
import java.util.EnumSet;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrderCapability;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexQuery;
import org.neo4j.internal.schema.IndexQuery.IndexQueryType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.IndexValueCapability;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.monitoring.Monitors;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.ValueCategory;

import static org.neo4j.internal.schema.IndexCapability.NO_CAPABILITY;
import static org.neo4j.internal.schema.IndexOrderCapability.NONE;
import static org.neo4j.internal.schema.IndexValueCapability.NO;

public class TextIndexProvider extends AbstractLuceneIndexProvider
{
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( "text", "1.0" );
    public static final IndexCapability CAPABILITY = new TextIndexCapability();

    public TextIndexProvider( FileSystemAbstraction fileSystem,
                              DirectoryFactory directoryFactory,
                              IndexDirectoryStructure.Factory directoryStructureFactory,
                              Monitors monitors, Config config,
                              DatabaseReadOnlyChecker readOnlyChecker )
    {
        super( IndexType.TEXT, DESCRIPTOR, fileSystem, directoryFactory, directoryStructureFactory, monitors, config, readOnlyChecker );
    }

    @Override
    public IndexDescriptor completeConfiguration( IndexDescriptor index )
    {
        return index.getCapability().equals( NO_CAPABILITY ) ? index.withIndexCapability( CAPABILITY ) : index;
    }

    @Override
    public IndexType getIndexType()
    {
        return IndexType.TEXT;
    }

    public static class TextIndexCapability implements IndexCapability
    {
        @Override
        public IndexOrderCapability orderCapability( ValueCategory... valueCategories )
        {
            return NONE;
        }

        @Override
        public IndexValueCapability valueCapability( ValueCategory... valueCategories )
        {
            return NO;
        }

        @Override
        public boolean areValueCategoriesAccepted( ValueCategory... valueCategories )
        {
            Preconditions.requireNonEmpty( valueCategories );
            Preconditions.requireNoNullElements( valueCategories );
            return valueCategories.length == 1 && valueCategories[0] == ValueCategory.TEXT;
        }

        @Override
        public boolean isQuerySupported( IndexQueryType queryType, ValueCategory valueCategory )
        {

            if ( queryType == IndexQueryType.ALL_ENTRIES )
            {
                return true;
            }

            if ( !areValueCategoriesAccepted( valueCategory ) )
            {
                return false;
            }

            switch ( queryType )
            {
            case EXACT:
            case STRING_PREFIX:
            case STRING_SUFFIX:
            case STRING_CONTAINS:
            case RANGE:
                return true;
            default:
                return false;
            }
        }

        @Override
        public double getCostMultiplier( IndexQueryType... queryTypes )
        {
            // for now, just make the operations which are more efficiently supported by
            // btree-based indexes slightly more expensive so the planner would choose a
            // btree-based index instead of lucene-based index if there is a choice
            if ( Arrays.stream( queryTypes ).anyMatch( EnumSet.of( IndexQueryType.EXACT, IndexQueryType.RANGE, IndexQueryType.STRING_PREFIX )::contains ) )
            {
                return 1.1;
            }
            return 1.0;
        }

        @Override
        public boolean supportPartitionedScan( IndexQuery... queries )
        {
            Preconditions.requireNonEmpty( queries );
            Preconditions.requireNoNullElements( queries );
            return false;
        }
    }
}
