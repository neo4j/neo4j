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

public class LuceneIndexProvider extends AbstractLuceneIndexProvider
{
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( "lucene", "2.0" );
    public static final IndexCapability CAPABILITY = new LuceneIndexCapability();

    public LuceneIndexProvider(
            FileSystemAbstraction fileSystem, DirectoryFactory directoryFactory, IndexDirectoryStructure.Factory directoryStructureFactory,
            Monitors monitors, Config config, DatabaseReadOnlyChecker readOnlyChecker )
    {
        super( IndexType.BTREE, DESCRIPTOR, fileSystem, directoryFactory, directoryStructureFactory, monitors, config, readOnlyChecker );
    }

    @Override
    public IndexDescriptor completeConfiguration( IndexDescriptor index )
    {
        return index.getCapability().equals( NO_CAPABILITY ) ? index.withIndexCapability( CAPABILITY ) : index;
    }

    public static class LuceneIndexCapability implements IndexCapability
    {

        @Override
        public IndexOrderCapability orderCapability( ValueCategory... valueCategories )
        {
            return IndexOrderCapability.NONE;
        }

        @Override
        public IndexValueCapability valueCapability( ValueCategory... valueCategories )
        {
            return IndexValueCapability.NO;
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
            return queryType != IndexQueryType.FULLTEXT_SEARCH
                   && queryType != IndexQueryType.TOKEN_LOOKUP
                   && areValueCategoriesAccepted( valueCategory );
        }

        @Override
        public double getCostMultiplier( IndexQueryType... queryTypes )
        {
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
