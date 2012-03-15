/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.util.Map;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterAccessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.PlaceboTm;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.*;

public class TestLuceneDataSource
{
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

    private IndexStore indexStore;
    private File datasourceDirectory;
    private LuceneDataSource dataSource;
    String dbPath = getDbPath();

    private String getDbPath()
    {
        return "target/var/datasource"+System.currentTimeMillis();
    }

    @Before
    public void setup()
    {
        datasourceDirectory = new File( dbPath );
        datasourceDirectory.mkdirs();
        indexStore = new IndexStore( dbPath, new DefaultFileSystemAbstraction() );
        addIndex( "foo" );
    }

    private void addIndex(String name)
    {
        indexStore.set( Node.class, name, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
    }

    private IndexIdentifier identifier(String name)
    {
        return new IndexIdentifier( LuceneCommand.NODE, dataSource.nodeEntityType, name);
    }

    @After
    public void teardown() throws IOException
    {
        dataSource.close();
        FileUtils.deleteRecursively( datasourceDirectory );
    }

    @Test
    public void testShouldReturnIndexWriterFromLRUCache() throws InstantiationException
    {
        dataSource = new LuceneDataSource(new Config( StringLogger.DEV_NULL, fileSystem, config() ), indexStore, new DefaultFileSystemAbstraction(),
                                          new XaFactory(new Config( StringLogger.DEV_NULL, fileSystem, config() ), TxIdGenerator.DEFAULT, new PlaceboTm(), CommonFactories.defaultLogBufferFactory(), new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL, CommonFactories.defaultRecoveryVerifier()) );
        IndexIdentifier identifier = identifier( "foo" );
        IndexWriter writer = dataSource.getIndexWriter( identifier );
        assertSame( writer, dataSource.getIndexWriter( identifier ) );
    }

    @Test
    public void testShouldReturnIndexSearcherFromLRUCache() throws InstantiationException
    {
        Config config = new Config( StringLogger.DEV_NULL, fileSystem, config() );
        dataSource = new LuceneDataSource( config, indexStore, new DefaultFileSystemAbstraction(),
                                           new XaFactory( config, TxIdGenerator.DEFAULT, new PlaceboTm(), CommonFactories.defaultLogBufferFactory(), new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL, CommonFactories.defaultRecoveryVerifier()) );
        IndexIdentifier identifier = identifier( "foo" );
        IndexWriter writer = dataSource.getIndexWriter( identifier );
        IndexSearcherRef searcher = dataSource.getIndexSearcher( identifier, false );
        assertSame( searcher, dataSource.getIndexSearcher( identifier, false ) );
    }

    @Test
    public void testClosesOldestIndexWriterWhenCacheSizeIsExceeded() throws InstantiationException
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String,String> config = config();
        config.put( GraphDatabaseSettings.lucene_writer_cache_size.name(), "2");
        Config config1 = new Config( StringLogger.DEV_NULL, fileSystem, config );
        dataSource = new LuceneDataSource( config1, indexStore, new DefaultFileSystemAbstraction(),
                                           new XaFactory(config1, TxIdGenerator.DEFAULT, new PlaceboTm(), CommonFactories.defaultLogBufferFactory(), new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL, CommonFactories.defaultRecoveryVerifier()) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter fooIndexWriter = dataSource.getIndexWriter( fooIdentifier );
        dataSource.getIndexWriter( barIdentifier );
        assertFalse( IndexWriterAccessor.isClosed( fooIndexWriter ) );
        dataSource.getIndexWriter( bazIdentifier );
        assertTrue( IndexWriterAccessor.isClosed( fooIndexWriter ) );
    }

    @Test
    public void testClosesOldestIndexSearcherWhenCacheSizeIsExceeded() throws InstantiationException
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String,String> config = config();
        config.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2");
        Config config1 = new Config( StringLogger.DEV_NULL, fileSystem, config );
        dataSource = new LuceneDataSource( config1, indexStore, new DefaultFileSystemAbstraction(),
                                           new XaFactory( config1, TxIdGenerator.DEFAULT, new PlaceboTm(), CommonFactories.defaultLogBufferFactory(), fileSystem, StringLogger.DEV_NULL, CommonFactories.defaultRecoveryVerifier()) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexSearcherRef fooSearcher = dataSource.getIndexSearcher( fooIdentifier, false );
        IndexSearcherRef barSearcher = dataSource.getIndexSearcher( barIdentifier, false );
        assertFalse( fooSearcher.isClosed() );
        IndexSearcherRef bazSearcher = dataSource.getIndexSearcher( bazIdentifier, false );
        assertTrue( fooSearcher.isClosed() );
    }

    @Test
    public void testRecreatesSearcherWhenRequestedAgain() throws InstantiationException
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String,String> config = config();
        config.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2");
        Config config1 = new Config( StringLogger.DEV_NULL, fileSystem, config );
        dataSource = new LuceneDataSource( config1, indexStore, new DefaultFileSystemAbstraction(),
                                           new XaFactory( config1, TxIdGenerator.DEFAULT, new PlaceboTm(), CommonFactories.defaultLogBufferFactory(), new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL, CommonFactories.defaultRecoveryVerifier()) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexSearcherRef oldFooSearcher = dataSource.getIndexSearcher( fooIdentifier, false );
        IndexSearcherRef barSearcher = dataSource.getIndexSearcher( barIdentifier, false );
        IndexSearcherRef bazSearcher = dataSource.getIndexSearcher( bazIdentifier, false );
        IndexSearcherRef newFooSearcher = dataSource.getIndexSearcher( bazIdentifier, false );
        assertNotSame( oldFooSearcher, newFooSearcher );
        assertFalse( newFooSearcher.isClosed() );
    }

    @Test
    public void testRecreatesWriterWhenRequestedAgainAfterCacheEviction() throws InstantiationException
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String,String> config = config();
        config.put( GraphDatabaseSettings.lucene_writer_cache_size.name(), "2");
        Config config1 = new Config( StringLogger.DEV_NULL, fileSystem, config );
        dataSource = new LuceneDataSource( config1, indexStore, new DefaultFileSystemAbstraction(),
                                           new XaFactory( config1, TxIdGenerator.DEFAULT, new PlaceboTm(), CommonFactories.defaultLogBufferFactory(), new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL, CommonFactories.defaultRecoveryVerifier()) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter oldFooIndexWriter = dataSource.getIndexWriter( fooIdentifier );
        dataSource.getIndexWriter( barIdentifier );
        dataSource.getIndexWriter( bazIdentifier );
        IndexWriter newFooIndexWriter = dataSource.getIndexWriter( fooIdentifier );
        assertNotSame( oldFooIndexWriter, newFooIndexWriter );
        assertFalse( IndexWriterAccessor.isClosed( newFooIndexWriter ) );
    }

    @Ignore( "No longer valid since Lucene 3.5" )
    @Test
    public void testInvalidatingSearcherCreatesANewOne() throws InstantiationException
    {
        Config config = new Config( StringLogger.DEV_NULL, fileSystem, config() );
        dataSource = new LuceneDataSource( config, indexStore, new DefaultFileSystemAbstraction(),
            new XaFactory( config, TxIdGenerator.DEFAULT, new PlaceboTm(), CommonFactories.defaultLogBufferFactory(), new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL, CommonFactories.defaultRecoveryVerifier()) );
        IndexIdentifier identifier = new IndexIdentifier( LuceneCommand.NODE, dataSource.nodeEntityType, "foo" );
        IndexSearcherRef oldSearcher = dataSource.getIndexSearcher( identifier, false );
        dataSource.invalidateIndexSearcher( identifier );
        IndexSearcherRef newSearcher = dataSource.getIndexSearcher( identifier, false );
        assertNotSame( oldSearcher, newSearcher );
        assertTrue( oldSearcher.isClosed() );
        assertFalse( newSearcher.isClosed() );
        assertNotSame( oldSearcher.getSearcher(), newSearcher.getSearcher() );
    }

    private Map<String,String> config()
    {
        return MapUtil.stringMap(
                "store_dir", getDbPath());
    }
}
