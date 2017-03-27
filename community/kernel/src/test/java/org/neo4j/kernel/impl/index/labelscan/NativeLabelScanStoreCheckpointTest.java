package org.neo4j.kernel.impl.index.labelscan;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.index.internal.gbptree.CheckpointCounter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.ControllableHealth;
import org.neo4j.kernel.Health;
import org.neo4j.kernel.api.labelscan.LoggingMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.labelscan.LoggingMonitor.SHUTDOWN_NO_FLUSH_MESSAGE;
import static org.neo4j.kernel.api.labelscan.LoggingMonitor.SHUTDOWN_WITH_FLUSH_MESSAGE;
import static org.neo4j.kernel.impl.api.scan.FullStoreChangeStream.EMPTY;

public class NativeLabelScanStoreCheckpointTest
{
    private FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory( getClass(), fileSystemRule.get() );
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private AssertableLogProvider logProvider = new AssertableLogProvider();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
            .around( fileSystemRule )
            .around( pageCacheRule )
            .around( logProvider );

    private CheckpointCounter checkpointCounter;
    private NativeLabelScanStore nativeLabelScanStore;
    private AtomicBoolean isHealthy;

    @Before
    public void setup()
    {
        Monitors monitors = new Monitors();
        checkpointCounter = new CheckpointCounter();
        LoggingMonitor loggingMonitor = new LoggingMonitor( logProvider.getLog( getClass() ) );
        monitors.addMonitorListener( checkpointCounter );
        monitors.addMonitorListener( loggingMonitor );

        isHealthy = new AtomicBoolean( true );
        Health health = new ControllableHealth( isHealthy );

        nativeLabelScanStore = nativeLabelScanStore( monitors, health );
    }

    @Test
    public void shouldCheckpointGBPTreeOnShutdown() throws Exception
    {
        // GIVEN
        nativeLabelScanStore.init();
        nativeLabelScanStore.start();
        checkpointCounter.reset();

        // WHEN
        nativeLabelScanStore.shutdown();

        // THEN
        assertThat( checkpointCounter.count(), is( 1 ) );
        logProvider.assertContainsLogCallContaining( SHUTDOWN_WITH_FLUSH_MESSAGE );
    }

    @Test
    public void shouldNotCheckpointGBPTreeOnShutdownIfNotStarted() throws Exception
    {
        // GIVEN
        nativeLabelScanStore.init();
        checkpointCounter.reset();

        // WHEN
        nativeLabelScanStore.shutdown();

        // THEN
        assertThat( checkpointCounter.count(), is( 0 ) );
        logProvider.assertContainsLogCallContaining( SHUTDOWN_NO_FLUSH_MESSAGE );
    }

    @Test
    public void shouldNotCheckpointGBPTreeOnShutdownIfPanic() throws Exception
    {
        // GIVEN
        nativeLabelScanStore.init();
        nativeLabelScanStore.start();
        isHealthy.set( false );
        checkpointCounter.reset();

        // WHEN
        nativeLabelScanStore.shutdown();

        // THEN
        assertThat( checkpointCounter.count(), is( 0 ) );
        logProvider.assertContainsLogCallContaining( SHUTDOWN_NO_FLUSH_MESSAGE );
    }

    private NativeLabelScanStore nativeLabelScanStore( Monitors monitors, Health health )
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
        File storeDir = testDirectory.graphDbDir();
        return new NativeLabelScanStore( pageCache, storeDir, EMPTY, false, monitors, health );
    }
}
