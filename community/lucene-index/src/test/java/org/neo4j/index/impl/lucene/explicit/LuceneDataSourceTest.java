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

import org.apache.lucene.index.IndexWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class LuceneDataSourceTest
{
    private final LifeRule life = new LifeRule( true );
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( directory ).around( fileSystemRule )
                                                 .around( life ).around( expectedException );

    private IndexConfigStore indexStore;
    private LuceneDataSource dataSource;

    @Before
    public void setUp()
    {
        indexStore = new IndexConfigStore( directory.directory(), fileSystemRule.get() );
        addIndex( "foo" );
    }

    @Test
    public void doNotTryToCommitWritersOnForceInReadOnlyMode() throws Exception
    {
        IndexIdentifier indexIdentifier = identifier( "foo" );
        prepareIndexesByIdentifiers( indexIdentifier );
        stopDataSource();

        Config readOnlyConfig = Config.defaults( readOnlyConfig() );
        LuceneDataSource readOnlyDataSource = life.add( getLuceneDataSource( readOnlyConfig ) );
        assertNotNull( readOnlyDataSource.getIndexSearcher( indexIdentifier ) );

        readOnlyDataSource.force();
    }

    @Test
    public void notAllowIndexDeletionInReadOnlyMode() throws Exception
    {
        IndexIdentifier indexIdentifier = identifier( "foo" );
        prepareIndexesByIdentifiers( indexIdentifier );
        stopDataSource();

        Config readOnlyConfig = Config.defaults( readOnlyConfig() );
        dataSource = life.add( getLuceneDataSource( readOnlyConfig, OperationalMode.single ) );
        expectedException.expect( IllegalStateException.class );
        expectedException.expectMessage("Index deletion in read only mode is not supported.");
        dataSource.deleteIndex( indexIdentifier, false );
    }

    @Test
    public void useReadOnlyIndexSearcherInReadOnlyModeForSingleInstance() throws Exception
    {
        IndexIdentifier indexIdentifier = identifier( "foo" );
        prepareIndexesByIdentifiers( indexIdentifier );
        stopDataSource();

        Config readOnlyConfig = Config.defaults( readOnlyConfig() );
        dataSource = life.add( getLuceneDataSource( readOnlyConfig, OperationalMode.single ) );

        IndexReference indexSearcher = dataSource.getIndexSearcher( indexIdentifier );
        assertTrue( "Read only index reference should be used in read only mode.",
                ReadOnlyIndexReference.class.isInstance( indexSearcher ) );
    }

    @Test
    public void useWritableIndexSearcherInReadOnlyModeForNonSingleInstance() throws Exception
    {
        IndexIdentifier indexIdentifier = identifier( "foo" );
        prepareIndexesByIdentifiers( indexIdentifier );
        stopDataSource();

        Config readOnlyConfig = Config.defaults( readOnlyConfig() );
        dataSource = life.add( getLuceneDataSource( readOnlyConfig, OperationalMode.ha ) );

        IndexReference indexSearcher = dataSource.getIndexSearcher( indexIdentifier );
        assertTrue( "Writable index reference should be used in read only mode in ha mode.",
                WritableIndexReference.class.isInstance( indexSearcher ) );
    }

    @Test
    public void refreshReadOnlyIndexSearcherInReadOnlyMode() throws Exception
    {
        IndexIdentifier indexIdentifier = identifier( "foo" );
        prepareIndexesByIdentifiers( indexIdentifier );
        stopDataSource();

        Config readOnlyConfig = Config.defaults( readOnlyConfig() );
        dataSource = life.add( getLuceneDataSource( readOnlyConfig ) );

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
        Config config = Config.defaults();
        dataSource = life.add( getLuceneDataSource( config ) );
        IndexIdentifier identifier = identifier( "foo" );
        IndexWriter writer = dataSource.getIndexSearcher( identifier ).getWriter();
        assertSame( writer, dataSource.getIndexSearcher( identifier ).getWriter() );
    }

    @Test
    public void testShouldReturnIndexSearcherFromLRUCache() throws Throwable
    {
        Config config = Config.defaults();
        dataSource = life.add( getLuceneDataSource( config ) );
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
        Config config = Config.defaults( cacheSizeConfig() );
        dataSource = life.add( getLuceneDataSource( config ) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter fooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        dataSource.getIndexSearcher( barIdentifier );
        assertTrue( fooIndexWriter.isOpen() );
        dataSource.getIndexSearcher( bazIdentifier );
        assertFalse( fooIndexWriter.isOpen() );
    }

    @Test
    public void testClosesOldestIndexSearcherWhenCacheSizeIsExceeded() throws Throwable
    {
        addIndex( "bar" );
        addIndex( "baz" );
        Config config = Config.defaults( cacheSizeConfig() );
        dataSource = life.add( getLuceneDataSource( config )  );
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
        Config config = Config.defaults( cacheSizeConfig() );
        dataSource = life.add( getLuceneDataSource( config ) );
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
        Config config = Config.defaults( cacheSizeConfig() );
        dataSource = life.add( getLuceneDataSource( config ) );
        IndexIdentifier fooIdentifier = identifier( "foo" );
        IndexIdentifier barIdentifier = identifier( "bar" );
        IndexIdentifier bazIdentifier = identifier( "baz" );
        IndexWriter oldFooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        dataSource.getIndexSearcher( barIdentifier );
        dataSource.getIndexSearcher( bazIdentifier );
        IndexWriter newFooIndexWriter = dataSource.getIndexSearcher( fooIdentifier ).getWriter();
        assertNotSame( oldFooIndexWriter, newFooIndexWriter );
        assertTrue( newFooIndexWriter.isOpen() );
    }

    private void stopDataSource() throws IOException
    {
        dataSource.shutdown();
    }

    private Map<String, String> config()
    {
        return stringMap();
    }

    private void prepareIndexesByIdentifiers( IndexIdentifier indexIdentifier ) throws Exception
    {
        Config config = Config.defaults();
        dataSource = life.add( getLuceneDataSource( config ) );
        dataSource.getIndexSearcher( indexIdentifier );
        dataSource.force();
    }

    private Map<String, String> readOnlyConfig()
    {
        return stringMap( GraphDatabaseSettings.read_only.name(), "true" );
    }

    private Map<String, String> cacheSizeConfig()
    {
        return stringMap( GraphDatabaseSettings.lucene_searcher_cache_size.name(), "2" );
    }

    private void addIndex( String name )
    {
        indexStore.set( Node.class, name, stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
    }

    private IndexIdentifier identifier( String name )
    {
        return new IndexIdentifier( IndexEntityType.Node, name );
    }

    private LuceneDataSource getLuceneDataSource( Config config )
    {
        return getLuceneDataSource( config, OperationalMode.unknown );
    }

    private LuceneDataSource getLuceneDataSource( Config config, OperationalMode operationalMode )
    {
        return new LuceneDataSource( directory.graphDbDir(), config, indexStore,
                new DefaultFileSystemAbstraction(), operationalMode );
    }
}
