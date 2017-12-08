package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.test.rule.NeoStoreDataSourceRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GetStoreRequestHandlerTest
{
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory( fs.get() );
    @Rule
    public NeoStoreDataSourceRule dsRule = new NeoStoreDataSourceRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private CheckPointer checkPointer = mock( CheckPointer.class );
    private StoreCopyCheckPointMutex mutex = mock( StoreCopyCheckPointMutex.class );
    private Resource checkPointLock = mock( Resource.class );

    private NeoStoreDataSource dataSource;
    private StoreId storeId;
    private GetStoreRequestHandler handler;
    private EmbeddedChannel channel;

    @Before
    public void setup() throws IOException
    {
        when( mutex.storeCopy( any() ) ).thenReturn( checkPointLock );

        PageCache pageCache = pageCacheRule.getPageCache( fs.get() );
        dataSource = dsRule.getDataSource( dir.graphDbDir(), fs.get(), pageCache );

        dataSource.init();
        dataSource.start();

        org.neo4j.kernel.impl.store.StoreId kernelStoreId = dataSource.getStoreId();
        storeId = new StoreId( kernelStoreId.getCreationTime(), kernelStoreId.getRandomId(), kernelStoreId.getUpgradeTime(), kernelStoreId.getUpgradeId() );

//        StoreStreamingProtocol storeStreamingProtocol = new StoreStreamingProtocol( pageCache, fs );
//        handler = new GetStoreRequestHandler( new CatchupServerProtocol(), () -> dataSource, storeStreamingProtocol );
        channel = new EmbeddedChannel( handler );
    }

    @After
    public void teardown()
    {
        dataSource.stop();
        dataSource.shutdown();
    }

    @Test
    public void shouldStreamStoreFiles() throws Exception
    {
        // when
        channel.writeInbound( new GetStoreRequest( storeId ) );

        // then
        int finishedCount = 0;
        int headerCount = 0;
        int senderCount = 0;
        int responseTypeCount = 0;

        Object in;
        while ( (in = channel.readOutbound()) != null )
        {
            if ( in instanceof FileHeader )
            {
                headerCount++;
            }
            else if ( in instanceof FileSender )
            {
                senderCount++;
            }
            else if ( in instanceof ResponseMessageType )
            {
                responseTypeCount++;
            }
            else if ( in instanceof StoreCopyFinishedResponse )
            {
                finishedCount++;
            }
            else
            {
                fail();
            }
        }

        long storeFileCount = dataSource.listStoreFiles( false ).stream().count();

        assertEquals( storeFileCount, headerCount );
        assertEquals( storeFileCount, senderCount );
        assertEquals( storeFileCount + 1, responseTypeCount );
        assertEquals( 1, finishedCount );
    }

    @Test
    public void shouldNotReleaseCheckpointLockUntilLastMessageWritten() throws Exception
    {
        // given
//        handler.channelRead0(  );

    }
}
