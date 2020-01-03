/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.schema.MultiTokenSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenRegistry;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor.UNDECIDED;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.readOrInitialiseDescriptor;
import static org.neo4j.storageengine.api.schema.IndexDescriptor.Type.GENERAL;

@ExtendWith( {TestDirectoryExtension.class, DefaultFileSystemExtension.class} )
class FulltextIndexSettingsTest
{
    @Inject
    TestDirectory directory;
    @Inject
    DefaultFileSystemAbstraction fs;

    @Test
    void shouldPersistFulltextIndexSettings() throws IOException
    {
        // Given
        File indexFolder = directory.directory( "indexFolder" );
        String analyzerName = "simple";
        String eventuallyConsistency = "true";
        String defaultAnalyzer = "defaultAnalyzer";
        int[] propertyIds = {1, 2, 3};
        MultiTokenSchemaDescriptor schema = SchemaDescriptorFactory.multiToken( new int[]{1, 2}, EntityType.NODE, propertyIds );

        // A fulltext index descriptor with configurations
        Properties properties = properties( analyzerName, eventuallyConsistency );
        FulltextSchemaDescriptor fulltextSchemaDescriptor = new FulltextSchemaDescriptor( schema, properties );
        StoreIndexDescriptor storeIndexDescriptor = storeIndexDescriptorFromSchema( fulltextSchemaDescriptor );
        TokenRegistry tokenRegistry = SimpleTokenHolder.createPopulatedTokenRegistry( TokenHolder.TYPE_PROPERTY_KEY, propertyIds );
        SimpleTokenHolder tokenHolder = new SimpleTokenHolder( tokenRegistry );
        FulltextIndexDescriptor fulltextIndexDescriptor = readOrInitialiseDescriptor( storeIndexDescriptor, defaultAnalyzer, tokenHolder, indexFolder, fs );
        assertEquals( analyzerName, fulltextIndexDescriptor.analyzerName() );
        assertEquals( Boolean.parseBoolean( eventuallyConsistency ), fulltextIndexDescriptor.isEventuallyConsistent() );

        // When persisting it
        FulltextIndexSettings.saveFulltextIndexSettings( fulltextIndexDescriptor, indexFolder, fs );

        // Then we should be able to load it back with settings being the same
        StoreIndexDescriptor loadingIndexDescriptor = storeIndexDescriptorFromSchema( schema );
        FulltextIndexDescriptor loadedDescriptor = readOrInitialiseDescriptor( loadingIndexDescriptor, defaultAnalyzer, tokenHolder, indexFolder, fs );
        assertEquals( fulltextIndexDescriptor.analyzerName(), loadedDescriptor.analyzerName() );
        assertEquals( fulltextIndexDescriptor.isEventuallyConsistent(), loadedDescriptor.isEventuallyConsistent() );
    }

    private StoreIndexDescriptor storeIndexDescriptorFromSchema( SchemaDescriptor schema )
    {
        return new IndexDescriptor( schema, GENERAL, Optional.of( "indexName" ), UNDECIDED ).withId( 1 );
    }

    private Properties properties( String analyzerName, String eventuallyConsistency )
    {
        Properties properties = new Properties();
        properties.putIfAbsent( FulltextIndexSettings.INDEX_CONFIG_ANALYZER, analyzerName );
        properties.putIfAbsent( FulltextIndexSettings.INDEX_CONFIG_EVENTUALLY_CONSISTENT, eventuallyConsistency );
        return properties;
    }
}
