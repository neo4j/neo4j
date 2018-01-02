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
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.IndexCommandFactory;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.LegacyIndexProviderTransaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

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
    private final IndexConfigStore indexStore;
    private final FileSystemAbstraction fileSystemAbstraction;

    public LuceneIndexImplementation( File storeDir, Config config, IndexConfigStore indexStore,
            FileSystemAbstraction fileSystemAbstraction )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.indexStore = indexStore;
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    @Override
    public void init() throws Throwable
    {
        this.dataSource = new LuceneDataSource( storeDir, config, indexStore, fileSystemAbstraction );
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
    public LegacyIndexProviderTransaction newTransaction( IndexCommandFactory commandFactory )
    {
        return new LuceneLegacyIndexTransaction( dataSource, commandFactory );
    }

    @Override
    public Map<String, String> fillInDefaults( Map<String, String> source )
    {
        Map<String, String> result = source != null ?
                new HashMap<>( source ) : new HashMap<String, String>();
        String analyzer = result.get( KEY_ANALYZER );
        if ( analyzer == null )
        {
            // Type is only considered if "analyzer" isn't supplied
            String type = result.get( KEY_TYPE );
            if ( type == null )
            {
                type = "exact";
                result.put( KEY_TYPE, type );
            }
            if ( type.equals( "fulltext" ) &&
                 !result.containsKey( LuceneIndexImplementation.KEY_TO_LOWER_CASE ) )
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
    public CommandHandler newApplier( boolean recovery )
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
