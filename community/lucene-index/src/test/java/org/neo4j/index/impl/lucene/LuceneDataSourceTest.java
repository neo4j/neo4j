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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterAccessor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LuceneDataSourceTest
{
    @Rule
    public final LifeRule life = new LifeRule( true );
    @Rule
    public final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private IndexConfigStore indexStore;
    private LuceneDataSource dataSource;

    @Before
    public void setUp()
    {
        indexStore = new IndexConfigStore( directory.directory(), new DefaultFileSystemAbstraction() );
        addIndex( "foo" );
    }

    @Test
    public void doNotTryToCommitWritersOnForceInReadOnlyMode() throws Throwable
    {
        IndexIdentifier indexIdentifier = identifier( "foo" );
        prepareIndexesByIdentifiers( indexIdentifier );
        stopDataSource();

        Config readOnlyConfig = new Config( readOnlyConfig(), GraphDatabaseSettings.class );
        LuceneDataSource readOnlyDataSource = life.add( new LuceneDataSource( directory.graphDbDir(), readOnlyConfig,
                indexStore, new DefaultFileSystemAbstraction() ) );
        assertNotNull( readOnlyDataSource.getIndexSearcher( indexIdentifier ) );

        readOnlyDataSource.force();
    }

    @Test
    public void notAllowIndexDeletionInReadOnlyMode() throws IOException
    {
        IndexIdentifier indexIdentifier = identifier( "foo" );
        prepareIndexesByIdentifiers( indexIdentifier );
        stopDataSource();

        Config readOnlyConfig = new Config( readOnlyConfig(), GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), readOnlyConfig, indexStore, new DefaultFileSystemAbstraction() ) );
        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage("Index deletion in read only mode is not supported.");
        dataSource.deleteIndex( indexIdentifier, false );
    }

    @Test
    public void useReadOnlyIndexSearcherInReadOnlyMode() throws IOException
    {
        IndexIdentifier indexIdentifier = identifier( "foo" );
        prepareIndexesByIdentifiers( indexIdentifier );
        stopDataSource();

        Config readOnlyConfig = new Config( readOnlyConfig(), GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), readOnlyConfig, indexStore, new DefaultFileSystemAbstraction() ) );

        IndexReference indexSearcher = dataSource.getIndexSearcher( indexIdentifier );
        assertTrue( "Read only index reference should be used in read only mode.",
                ReadOnlyIndexReference.class.isInstance( indexSearcher ) );
    }

    @Test
    public void refreshReadOnlyIndexSearcherInReadOnlyMode() throws IOException
    {
        IndexIdentifier indexIdentifier = identifier( "foo" );
        prepareIndexesByIdentifiers( indexIdentifier );
        stopDataSource();

        Config readOnlyConfig = new Config( readOnlyConfig(), GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), readOnlyConfig, indexStore, new DefaultFileSystemAbstraction() ) );

        IndexReference indexSearcher = dataSource.getIndexSearcher( indexIdentifier );
        IndexReference indexSearcher2 = dataSource.getIndexSearcher( indexIdentifier );
        IndexReference indexSearcher3 = dataSource.getIndexSearcher( indexIdentifier );
        IndexReference indexSearcher4 = dataSource.getIndexSearcher( indexIdentifier );
        assertSame( "Refreshed read only searcher should be the same.", indexSearcher, indexSearcher2 );
        assertSame( "Refreshed read only searcher should be the same.", indexSearcher2, indexSearcher3 );
        assertSame( "Refreshed read only searcher should be the same.", indexSearcher3, indexSearcher4 );
    }

    @Test
    public void testShouldReturnIndexWriterFromLRUCache() throws Throwable
    {
        Config config = new Config( config(), GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier identifier = identifier( "foo" );
        IndexWriter writer = dataSource.getIndexSearcher( identifier ).getWriter();
        assertSame( writer, dataSource.getIndexSearcher( identifier ).getWriter() );
    }

    @Test
    public void testShouldReturnIndexSearcherFromLRUCache() throws Throwable
    {
        Config config = new Config( config(), GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier identifier = identifier( "foo" );
        IndexReference searcher = dataSource.getIndexSearcher( identifier );
        assertSame( searcher, dataSource.getIndexSearcher( identifier ) );
        searcher.close();
    }

    @Test
    public void testClosesOldestIndexWriterWhenCacheSizeIsExceeded() throws Throwable
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter fooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        dataSource.getIndexSearcher( barIdentifier );
        assertFalse( IndexWriterAccessor.isClosed( fooIndexWriter ) );
        dataSource.getIndexSearcher( bazIdentifier );
        assertTrue( IndexWriterAccessor.isClosed( fooIndexWriter ) );
    }

    @Test
    public void testClosesOldestIndexSearcherWhenCacheSizeIsExceeded() throws Throwable
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexReference fooSearcher = dataSource.getIndexSearcher( fooIdentifier );
        IndexReference barSearcher = dataSource.getIndexSearcher( barIdentifier );
        assertFalse( fooSearcher.isClosed() );
        IndexReference bazSearcher = dataSource.getIndexSearcher( bazIdentifier );
        assertTrue( fooSearcher.isClosed() );
        barSearcher.close();
        bazSearcher.close();
    }

    @Test
    public void testRecreatesSearcherWhenRequestedAgain() throws Throwable
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexReference oldFooSearcher = dataSource.getIndexSearcher( fooIdentifier );
        IndexReference barSearcher = dataSource.getIndexSearcher( barIdentifier );
        IndexReference bazSearcher = dataSource.getIndexSearcher( bazIdentifier );
        IndexReference newFooSearcher = dataSource.getIndexSearcher( bazIdentifier );
        assertNotSame( oldFooSearcher, newFooSearcher );
        assertFalse( newFooSearcher.isClosed() );
        oldFooSearcher.close();
        barSearcher.close();
        bazSearcher.close();
        newFooSearcher.close();
    }

    @Test
    public void testRecreatesWriterWhenRequestedAgainAfterCacheEviction() throws Throwable
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Map<String, String> configMap = config();
        configMap.put( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
        Config config = new Config( configMap, GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter oldFooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        dataSource.getIndexSearcher( barIdentifier );
        dataSource.getIndexSearcher( bazIdentifier );
        IndexWriter newFooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        assertNotSame( oldFooIndexWriter, newFooIndexWriter );
        assertFalse( IndexWriterAccessor.isClosed( newFooIndexWriter ) );
    }

    private Map<String, String> config()
    {
        return MapUtil.stringMap();
    }

    private void prepareIndexesByIdentifiers( IndexIdentifier indexIdentifier )
    {
        Config config = new Config( config(), GraphDatabaseSettings.class );
        dataSource = life.add( new LuceneDataSource( directory.graphDbDir(), config, indexStore, new DefaultFileSystemAbstraction() ) );
        dataSource.getIndexSearcher( indexIdentifier );
        dataSource.force();
    }

    private Map<String, String> readOnlyConfig()
    {
        Map<String,String> config = config();
        config.put( GraphDatabaseSettings.read_only.name(), "true" );
        return config;
    }

    private void addIndex( String name )
    {
        indexStore.set( Node.class, name, MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
    }

    private IndexIdentifier identifier( String name )
    {
        return new IndexIdentifier( IndexEntityType.Node, name );
    }

    private void stopDataSource() throws IOException
    {
        try
        {
            dataSource.stop();
            dataSource.shutdown();
        }
        catch ( Throwable e )
        {
            throw Exceptions.launderedException( IOException.class, e );
        }
    }
}
