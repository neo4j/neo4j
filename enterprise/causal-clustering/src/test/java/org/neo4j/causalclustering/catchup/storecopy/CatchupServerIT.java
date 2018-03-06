/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.causalclustering.StrippedCatchupServer;
import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.catchup.storecopy.RealStrippedCatchupServer.getCheckPointer;
import static org.neo4j.causalclustering.catchup.storecopy.RealStrippedCatchupServer.getNeoStoreDataSource;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.fs.FileUtils.relativePath;

public class CatchupServerIT
{
    private static final String EXISTING_FILE_NAME = "neostore.nodestore.db";
    private static final StoreId WRONG_STORE_ID = new StoreId( 123, 221, 3131, 45678 );
    private static final LogProvider LOG_PROVIDER = NullLogProvider.getInstance();

    private static final String PROP_NAME = "name";
    private static final String PROP = "prop";
    public static final Label LABEL = label( "MyLabel" );
    private static final String INDEX = "index";
    private GraphDatabaseAPI graphDb;
    private StrippedCatchupServer catchupServer;
    private File temporaryDirectory;

    private PageCache pageCache;

    @Rule
    public DefaultFileSystemRule fsa = new DefaultFileSystemRule();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fsa );
    private CatchUpClient catchUpClient;

    @Before
    public void startDb()
    {
        temporaryDirectory = testDirectory.directory();
        graphDb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( fsa ).newEmbeddedDatabase( testDirectory.graphDbDir() );
        createLegacyIndex();
        createPropertyIndex();
        addData( graphDb );
        catchupServer = new RealStrippedCatchupServer( fsa, graphDb );
        catchupServer.before();
        catchupServer.start();
        catchUpClient = new CatchUpClient( LOG_PROVIDER, Clock.systemUTC(), 10000, new Monitors(), VoidPipelineWrapperFactory.VOID_WRAPPER );
        catchUpClient.start();
        pageCache = graphDb.getDependencyResolver().resolveDependency( PageCache.class );
    }

    @After
    public void stopDb() throws IOException
    {
        pageCache.flushAndForce();
        if ( graphDb != null )
        {
            graphDb.shutdown();
        }
        Optional.ofNullable( catchupServer ).ifPresent( StrippedCatchupServer::stop );
        Optional.ofNullable( catchUpClient ).ifPresent( CatchUpClient::stop );
    }

    @Test
    public void shouldListExpectedFilesCorrectly() throws Exception
    {
        // given (setup) required runtime subject dependencies
        NeoStoreDataSource neoStoreDataSource = getNeoStoreDataSource( graphDb );
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, fsa, catchUpClient, catchupServer, temporaryDirectory, LOG_PROVIDER );

        // when
        PrepareStoreCopyResponse prepareStoreCopyResponse = simpleCatchupClient.requestListOfFilesFromServer();
        simpleCatchupClient.close();

        // then
        listOfDownloadedFilesMatchesServer( neoStoreDataSource, prepareStoreCopyResponse.getFiles() );

        // and downloaded files are identical to source
        List<File> expectedCountStoreFiles = listServerExpectedNonReplayableFiles( neoStoreDataSource );
        for ( File snapshotedStorefile : expectedCountStoreFiles )
        {
            fileContentEquals( databaseFileToClientFile( snapshotedStorefile ), snapshotedStorefile );
        }

        // and
        assertTransactionIdMatches( prepareStoreCopyResponse.lastTransactionId() );

        //and
        assertDescriptorsMatch( prepareStoreCopyResponse.getDescriptors(), neoStoreDataSource );
    }

    @Test
    public void shouldCommunicateErrorIfStoreIdDoesNotMatchRequest() throws Exception
    {
        // given (setup) required runtime subject dependencies
        addData( graphDb );
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, fsa, catchUpClient, catchupServer, temporaryDirectory, LOG_PROVIDER );

        // when the list of files are requested from the server with the wrong storeId
        PrepareStoreCopyResponse prepareStoreCopyResponse = simpleCatchupClient.requestListOfFilesFromServer( WRONG_STORE_ID );
        simpleCatchupClient.close();

        // then the response is not a list of files but an error
        assertEquals( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH, prepareStoreCopyResponse.status() );

        // and the list of files is empty because the request should have failed
        File[] remoteFiles = prepareStoreCopyResponse.getFiles();
        assertArrayEquals( new File[]{}, remoteFiles );
    }

    @Test
    public void individualFileCopyWorks() throws Exception
    {
        // given a file exists on the server
        addData( graphDb );
        File existingFile = new File( temporaryDirectory, EXISTING_FILE_NAME );

        // and
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, fsa, catchUpClient, catchupServer, temporaryDirectory, LOG_PROVIDER );

        // when we copy that file
        pageCache.flushAndForce();
        StoreCopyFinishedResponse storeCopyFinishedResponse = simpleCatchupClient.requestIndividualFile( existingFile );
        simpleCatchupClient.close();

        // then the response is successful
        assertEquals( StoreCopyFinishedResponse.Status.SUCCESS, storeCopyFinishedResponse.status() );

        // then the contents matches
        fileContentEquals( clientFileToDatabaseFile( existingFile ), existingFile );
    }

    @Test
    public void individualIndexSnapshotCopyWorks() throws Exception
    {

        // given
        NeoStoreDataSource neoStoreDataSource = getNeoStoreDataSource( graphDb );
        List<File> expectingFiles = neoStoreDataSource.getNeoStoreFileListing().builder().excludeAll().includeSchemaIndexStoreFiles().build().stream().map(
                StoreFileMetadata::file ).collect( toList() );
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, fsa, catchUpClient, catchupServer, temporaryDirectory, LOG_PROVIDER );

        // and
        Collection<IndexDescriptor> expectedDescriptors = getExpectedDescriptors( neoStoreDataSource );

        // when
        for ( IndexDescriptor expectedDescriptor : expectedDescriptors )
        {
            StoreCopyFinishedResponse response = simpleCatchupClient.requestIndexSnapshot( expectedDescriptor );
            simpleCatchupClient.close();
            assertEquals( StoreCopyFinishedResponse.Status.SUCCESS, response.status() );
        }

        // then
        fileContentEquals( expectingFiles );
    }

    @Test
    public void individualFileCopyFailsIfStoreIdMismatch() throws Exception
    {
        // given a file exists on the server
        addData( graphDb );
        File expectedExistingFile = new File( graphDb.getStoreDir(), EXISTING_FILE_NAME );

        // and
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, fsa, catchUpClient, catchupServer, temporaryDirectory, LOG_PROVIDER );

        // when we copy that file using a different storeId
        StoreCopyFinishedResponse storeCopyFinishedResponse = simpleCatchupClient.requestIndividualFile( expectedExistingFile, WRONG_STORE_ID );
        simpleCatchupClient.close();

        // then the response from the server should be an error message that describes a store ID mismatch
        assertEquals( StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH, storeCopyFinishedResponse.status() );
    }

    private void assertTransactionIdMatches( long lastTxId )
    {
        long expectedTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        assertEquals( expectedTransactionId, lastTxId);
    }

    private void fileContentEquals( Collection<File> countStore ) throws IOException
    {
        for ( File file : countStore )
        {
            fileContentEquals( databaseFileToClientFile( file ), file );
        }
    }

    private File databaseFileToClientFile( File file ) throws IOException
    {
        String relativePathToDatabaseDir = relativePath( new File( temporaryDirectory, "graph-db" ), file );
        return new File( temporaryDirectory, relativePathToDatabaseDir );
    }

    private File clientFileToDatabaseFile( File file ) throws IOException
    {
        String relativePathToDatabaseDir = relativePath( temporaryDirectory, file );
        return new File( new File( temporaryDirectory, "graph-db" ), relativePathToDatabaseDir );
    }

    private void fileContentEquals( File fileA, File fileB )
    {
        assertNotEquals( fileA.getPath(), fileB.getPath() );
        String message = String.format( "Expected file: %s\ndoes not match actual file: %s", fileA, fileB );
        assertEquals( message, StoreCopyClientIT.fileContent( fileA, fsa ), StoreCopyClientIT.fileContent( fileB, fsa ) );
    }

    private void listOfDownloadedFilesMatchesServer( NeoStoreDataSource neoStoreDataSource, File[] files )
            throws IOException
    {
        List<String> expectedStoreFiles = getExpectedStoreFiles( neoStoreDataSource );
        List<String> givenFile = Arrays.stream( files ).map( File::getName ).collect( toList() );
        assertThat( givenFile, containsInAnyOrder( expectedStoreFiles.toArray( new String[givenFile.size()] ) ) );
    }

    private void assertDescriptorsMatch( IndexDescriptor[] descriptors, NeoStoreDataSource neoStoreDataSource )
    {
        Collection<IndexDescriptor> expectedDescriptors = getExpectedDescriptors( neoStoreDataSource );

        assertThat( expectedDescriptors, containsInAnyOrder( descriptors ) );
        assertThat( expectedDescriptors.size(), equalTo( descriptors.length ) );
    }

    private Collection<IndexDescriptor> getExpectedDescriptors( NeoStoreDataSource neoStoreDataSource )
    {
        return neoStoreDataSource.getNeoStoreFileListing().getNeoStoreFileIndexListing().listIndexDescriptors();
    }

    private List<File> listServerExpectedNonReplayableFiles( NeoStoreDataSource neoStoreDataSource ) throws IOException
    {
        try ( Stream<StoreFileMetadata> countStoreStream = neoStoreDataSource.getNeoStoreFileListing().builder().excludeAll()
                .includeNeoStoreFiles().build().stream();
                Stream<StoreFileMetadata> explicitIndexStream = neoStoreDataSource.getNeoStoreFileListing().builder().excludeAll()
                         .includeExplicitIndexStoreStoreFiles().build().stream() )
        {
            return Stream.concat( countStoreStream.filter( isCountFile() ), explicitIndexStream ).map( StoreFileMetadata::file ).collect( toList() );
        }
    }

    private List<String> getExpectedStoreFiles( NeoStoreDataSource neoStoreDataSource ) throws IOException
    {
        try ( Stream<StoreFileMetadata> stream = neoStoreDataSource.getNeoStoreFileListing().builder()
                .excludeLogFiles().excludeExplicitIndexStoreFiles().excludeSchemaIndexStoreFiles().build().stream() )
        {
            return stream.filter( isCountFile().negate() ).map( sfm -> sfm.file().getName() ).collect( toList() );
        }
    }

    private static Predicate<StoreFileMetadata> isCountFile()
    {
        return storeFileMetadata -> StoreType.typeOf( storeFileMetadata.file().getName() ).filter( f -> f == StoreType.COUNTS ).isPresent();
    }

    private void addData( GraphDatabaseAPI graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            node.addLabel( LABEL );
            node.setProperty( PROP_NAME, "Neo" );
            node.setProperty( PROP, Math.random() * 10000 );
            graphDb.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
    }

    private void createPropertyIndex()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( LABEL ).on( PROP_NAME ).create();
            tx.success();
        }
    }

    private void createLegacyIndex()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Index<Node> nodeIndex = graphDb.index().forNodes( INDEX );
            nodeIndex.add( graphDb.createNode(), "some-key", "som-value" );
            tx.success();
        }
    }
}
