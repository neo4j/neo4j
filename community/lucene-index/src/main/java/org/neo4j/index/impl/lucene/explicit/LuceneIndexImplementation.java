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
package org.neo4j.index.impl.lucene.explicit;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.spi.explicitindex.ExplicitIndexProviderTransaction;
import org.neo4j.kernel.spi.explicitindex.IndexCommandFactory;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;

public class LuceneIndexImplementation extends LifecycleAdapter implements IndexImplementation
{
    static final String KEY_TYPE = "type";
    static final String KEY_ANALYZER = "analyzer";
    static final String KEY_TO_LOWER_CASE = "to_lower_case";
    static final String KEY_SIMILARITY = "similarity";
    public static final String SERVICE_NAME = "lucene";

    public static final Map<String, String> EXACT_CONFIG =
            Collections.unmodifiableMap( MapUtil.stringMap(
                    IndexManager.PROVIDER, SERVICE_NAME, KEY_TYPE, "exact" ) );

    public static final Map<String, String> FULLTEXT_CONFIG =
            Collections.unmodifiableMap( MapUtil.stringMap(
                    IndexManager.PROVIDER, SERVICE_NAME, KEY_TYPE, "fulltext",
                    KEY_TO_LOWER_CASE, "true" ) );

    private LuceneDataSource dataSource;
    private final File storeDir;
    private final Config config;
    private final Supplier<IndexConfigStore> indexStore;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final OperationalMode operationalMode;

    public LuceneIndexImplementation( File storeDir, Config config, Supplier<IndexConfigStore> indexStore,
            FileSystemAbstraction fileSystemAbstraction, OperationalMode operationalMode )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.indexStore = indexStore;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.operationalMode = operationalMode;
    }

    @Override
    public void init()
    {
        this.dataSource = new LuceneDataSource( storeDir, config, indexStore.get(), fileSystemAbstraction, operationalMode );
        this.dataSource.init();
    }

    @Override
    public void start() throws Throwable
    {
        this.dataSource.start();
    }

    @Override
    public void stop() throws Throwable
    {
        this.dataSource.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        this.dataSource.shutdown();
        this.dataSource = null;
    }

    @Override
    public File getIndexImplementationDirectory( File storeDir )
    {
        return LuceneDataSource.getLuceneIndexStoreDirectory(storeDir);
    }

    @Override
    public ExplicitIndexProviderTransaction newTransaction( IndexCommandFactory commandFactory )
    {
        return new LuceneExplicitIndexTransaction( dataSource, commandFactory );
    }

    @Override
    public Map<String, String> fillInDefaults( Map<String, String> source )
    {
        Map<String, String> result = source != null ?
                new HashMap<>( source ) : new HashMap<>();
        String analyzer = result.get( KEY_ANALYZER );
        if ( analyzer == null )
        {
            // Type is only considered if "analyzer" isn't supplied
            String type = result.computeIfAbsent( KEY_TYPE, k -> "exact" );
            if ( type.equals( "fulltext" ) && !result.containsKey( LuceneIndexImplementation.KEY_TO_LOWER_CASE ) )
            {
                result.put( KEY_TO_LOWER_CASE, "true" );
            }
        }

        // Try it on for size. Calling this will reveal configuration problems.
        IndexType.getIndexType( result );

        return result;
    }

    @Override
    public boolean configMatches( Map<String, String> storedConfig, Map<String, String> config )
    {
        return  match( storedConfig, config, KEY_TYPE, null ) &&
                match( storedConfig, config, KEY_TO_LOWER_CASE, "true" ) &&
                match( storedConfig, config, KEY_ANALYZER, null ) &&
                match( storedConfig, config, KEY_SIMILARITY, null );
    }

    private boolean match( Map<String, String> storedConfig, Map<String, String> config,
            String key, String defaultValue )
    {
        String value1 = storedConfig.get( key );
        String value2 = config.get( key );
        if ( value1 == null || value2 == null )
        {
            if ( value1 == value2 )
            {
                return true;
            }
            if ( defaultValue != null )
            {
                value1 = value1 != null ? value1 : defaultValue;
                value2 = value2 != null ? value2 : defaultValue;
                return value1.equals( value2 );
            }
        }
        else
        {
            return value1.equals( value2 );
        }
        return false;
    }

    @Override
    public TransactionApplier newApplier( boolean recovery )
    {
        return new LuceneCommandApplier( dataSource, recovery );
    }

    @Override
    public ResourceIterator<File> listStoreFiles() throws IOException
    {
        return dataSource.listStoreFiles();
    }

    @Override
    public void force()
    {
        dataSource.force();
    }
}
