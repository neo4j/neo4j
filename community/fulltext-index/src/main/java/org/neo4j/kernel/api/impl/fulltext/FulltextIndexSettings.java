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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.eclipse.collections.api.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.schema.AnalyzerProvider;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class FulltextIndexSettings
{
    private static final String INDEX_CONFIG_FILE = "fulltext-index.properties";
    static final String INDEX_CONFIG_ANALYZER = "analyzer";
    static final String INDEX_CONFIG_EVENTUALLY_CONSISTENT = "eventually_consistent";

    static FulltextIndexDescriptor readOrInitialiseDescriptor( StorageIndexReference descriptor, String defaultAnalyzerName,
            TokenHolder propertyKeyTokenHolder, PartitionedIndexStorage indexStorage, FileSystemAbstraction fileSystem )
    {
        Properties properties = new Properties();
        FulltextSchemaDescriptor schema = descriptor.schema().asFulltextSchemaDescriptor();
        IndexConfig indexConfig = schema.getIndexConfig();
        for ( Pair<String,Value> entry : indexConfig.entries() )
        {
            properties.put( entry.getOne(), String.valueOf( entry.getTwo().asObject() ) );
        }
        loadPersistedSettings( properties, indexStorage, fileSystem );
        boolean eventuallyConsistent = Boolean.parseBoolean( properties.getProperty( INDEX_CONFIG_EVENTUALLY_CONSISTENT ) );
        String analyzerName = properties.getProperty( INDEX_CONFIG_ANALYZER, defaultAnalyzerName );
        Analyzer analyzer = createAnalyzer( analyzerName );
        List<String> names = new ArrayList<>();
        for ( int propertyKeyId : schema.getPropertyIds() )
        {
            try
            {
                names.add( propertyKeyTokenHolder.getTokenById( propertyKeyId ).name() );
            }
            catch ( TokenNotFoundException e )
            {
                throw new IllegalStateException( "Property key id not found.",
                        new PropertyKeyIdNotFoundKernelException( propertyKeyId, e ) );
            }
        }
        String[] propertyNames = names.toArray( new String[0] );
        return new FulltextIndexDescriptor( descriptor, propertyNames, analyzer, analyzerName, eventuallyConsistent );
    }

    private static void loadPersistedSettings( Properties settings, PartitionedIndexStorage indexStorage, FileSystemAbstraction fs )
    {
        File settingsFile = new File( indexStorage.getIndexFolder(), INDEX_CONFIG_FILE );
        if ( fs.fileExists( settingsFile ) )
        {
            try ( Reader reader = fs.openAsReader( settingsFile, StandardCharsets.UTF_8 ) )
            {
                settings.load( reader );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( "Failed to read persisted fulltext index properties: " + settingsFile, e );
            }
        }
    }

    static Analyzer createAnalyzer( String analyzerName )
    {
        try
        {
            return Services.loadOrFail( AnalyzerProvider.class, analyzerName ).createAnalyzer();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create fulltext analyzer: " + analyzerName, e );
        }
    }

    static Analyzer createAnalyzer( IndexDescriptor descriptor, TokenNameLookup tokenNameLookup )
    {
        TextValue analyzerName = descriptor.schema().getIndexConfig().get( INDEX_CONFIG_ANALYZER );
        if ( analyzerName == null )
        {
            throw new RuntimeException( "Index has no analyzer configured: " + descriptor.userDescription( tokenNameLookup ) );
        }
        try
        {
            return Services.loadOrFail( AnalyzerProvider.class, analyzerName.stringValue() ).createAnalyzer();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create fulltext analyzer: " + analyzerName, e );
        }
    }

    static String[] createPropertyNames( IndexDescriptor descriptor, TokenNameLookup tokenNameLookup )
    {
        int[] propertyIds = descriptor.schema().getPropertyIds();
        String[] propertyNames = new String[propertyIds.length];
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            propertyNames[i] = tokenNameLookup.propertyKeyGetName( propertyIds[i] );
        }
        return propertyNames;
    }

    static IndexConfig toIndexConfig( Map<String,String> map )
    {
        IndexConfig config = IndexConfig.empty();

        String analyzer = map.remove( INDEX_CONFIG_ANALYZER );
        if ( analyzer != null )
        {
            config = config.with( INDEX_CONFIG_ANALYZER, Values.stringValue( analyzer ) );
        }

        String eventuallyConsistent = map.remove( INDEX_CONFIG_EVENTUALLY_CONSISTENT );
        if ( eventuallyConsistent != null )
        {
            config = config.with( INDEX_CONFIG_EVENTUALLY_CONSISTENT, Values.booleanValue( Boolean.parseBoolean( eventuallyConsistent ) ) );
        }

        // Ignore any other entries that the map might contain.

        return config;
    }
}
