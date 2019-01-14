/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.store.format;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.neo4j.kernel.impl.store.format.highlimit.v306.HighLimitV3_0_6;
import org.neo4j.kernel.impl.store.format.highlimit.v310.HighLimitV3_1_0;
import org.neo4j.kernel.impl.store.format.highlimit.v320.HighLimitV3_2_0;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.findSuccessor;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForConfig;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStore;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectNewestFormat;

public class RecordFormatSelectorTest
{
    private static final LogProvider LOG = NullLogProvider.getInstance();

    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( pageCacheRule ).around( fileSystemRule );

    private final FileSystemAbstraction fs = fileSystemRule.get();
    private final File storeDir = new File( "graph.db" );

    @Test
    public void defaultFormatTest()
    {
        assertSame( Standard.LATEST_RECORD_FORMATS, defaultFormat() );
    }

    @Test
    public void selectForVersionTest()
    {
        assertSame( StandardV2_3.RECORD_FORMATS, selectForVersion( StandardV2_3.STORE_VERSION ) );
        assertSame( StandardV3_0.RECORD_FORMATS, selectForVersion( StandardV3_0.STORE_VERSION ) );
        assertSame( StandardV3_2.RECORD_FORMATS, selectForVersion( StandardV3_2.STORE_VERSION ) );
        assertSame( StandardV3_4.RECORD_FORMATS, selectForVersion( StandardV3_4.STORE_VERSION ) );
        assertSame( HighLimitV3_0_0.RECORD_FORMATS, selectForVersion( HighLimitV3_0_0.STORE_VERSION ) );
        assertSame( HighLimitV3_1_0.RECORD_FORMATS, selectForVersion( HighLimitV3_1_0.STORE_VERSION ) );
        assertSame( HighLimit.RECORD_FORMATS, selectForVersion( HighLimit.STORE_VERSION ) );
    }

    @Test
    public void selectForWrongVersionTest()
    {
        try
        {
            selectForVersion( "vA.B.9" );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void selectForConfigWithRecordFormatParameter()
    {
        assertSame( Standard.LATEST_RECORD_FORMATS, selectForConfig( config( Standard.LATEST_NAME ), LOG ) );
        assertSame( HighLimit.RECORD_FORMATS, selectForConfig( config( HighLimit.NAME ), LOG ) );
    }

    @Test
    public void selectForConfigWithoutRecordFormatParameter()
    {
        assertSame( defaultFormat(), selectForConfig( Config.defaults(), LOG ) );
    }

    @Test
    public void selectForConfigWithWrongRecordFormatParameter()
    {
        try
        {
            selectForConfig( config( "unknown_format" ), LOG );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void selectForStoreWithValidStore() throws IOException
    {
        PageCache pageCache = getPageCache();
        verifySelectForStore( pageCache, StandardV2_3.RECORD_FORMATS );
        verifySelectForStore( pageCache, StandardV3_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimitV3_0_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimit.RECORD_FORMATS );
    }

    @Test
    public void selectForStoreWithNoStore()
    {
        assertNull( selectForStore( storeDir, getPageCache(), LOG ) );
    }

    @Test
    public void selectForStoreWithThrowingPageCache() throws IOException
    {
        createNeoStoreFile();
        PageCache pageCache = mock( PageCache.class );
        when( pageCache.getCachedFileSystem() ).thenReturn( fs );
        when( pageCache.pageSize() ).thenReturn( PageCache.PAGE_SIZE );
        when( pageCache.map( any(), anyInt(), any() ) ).thenThrow( new IOException( "No reading..." ) );
        assertNull( selectForStore( storeDir, pageCache, LOG ) );
        verify( pageCache ).map( any(), anyInt(), any() );
    }

    @Test
    public void selectForStoreWithInvalidStoreVersion() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( "v9.Z.9", pageCache );
        assertNull( selectForStore( storeDir, getPageCache(), LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithSameStandardConfiguredAndStoredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = config( Standard.LATEST_NAME );

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithSameHighLimitConfiguredAndStoredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = config( HighLimit.NAME );

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithDifferentlyConfiguredAndStoredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = config( HighLimit.NAME );

        try
        {
            selectForStoreOrConfig( config, storeDir, pageCache, LOG );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void selectForStoreOrConfigWithOnlyStandardStoredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithOnlyHighLimitStoredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithOnlyStandardConfiguredFormat()
    {
        PageCache pageCache = getPageCache();

        Config config = config( Standard.LATEST_NAME );

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithOnlyHighLimitConfiguredFormat()
    {
        PageCache pageCache = getPageCache();

        Config config = config( HighLimit.NAME );

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithWrongConfiguredFormat()
    {
        PageCache pageCache = getPageCache();

        Config config = config( "unknown_format" );

        try
        {
            selectForStoreOrConfig( config, storeDir, pageCache, LOG );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void selectForStoreOrConfigWithoutConfiguredAndStoredFormats()
    {
        assertSame( defaultFormat(), selectForStoreOrConfig( Config.defaults(), storeDir, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatWithConfiguredStandardFormat()
    {
        assertSame( Standard.LATEST_RECORD_FORMATS,
                selectNewestFormat( config( Standard.LATEST_NAME ), storeDir, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatWithConfiguredHighLimitFormat()
    {
        assertSame( HighLimit.RECORD_FORMATS,
                selectNewestFormat( config( HighLimit.NAME ), storeDir, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatWithWrongConfiguredFormat()
    {
        try
        {
            selectNewestFormat( config( "unknown_format" ), storeDir, getPageCache(), LOG );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void selectNewestFormatWithoutConfigAndStore()
    {
        assertSame( defaultFormat(), selectNewestFormat( Config.defaults(), storeDir, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatForExistingStandardStore() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( Standard.LATEST_RECORD_FORMATS, selectNewestFormat( config, storeDir, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatForExistingHighLimitStore() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( HighLimit.RECORD_FORMATS, selectNewestFormat( config, storeDir, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatForExistingStoreWithLegacyFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( StandardV2_3.STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( defaultFormat(), selectNewestFormat( config, storeDir, getPageCache(), LOG ) );
    }

    @Test
    public void findSuccessorLatestVersion()
    {
        assertFalse( findSuccessor( defaultFormat() ).isPresent() );
    }

    @Test
    public void findSuccessorToOlderVersion()
    {
        assertEquals( StandardV3_0.RECORD_FORMATS, findSuccessor( StandardV2_3.RECORD_FORMATS ).get() );
        assertEquals( StandardV3_2.RECORD_FORMATS, findSuccessor( StandardV3_0.RECORD_FORMATS ).get() );
        assertEquals( StandardV3_4.RECORD_FORMATS, findSuccessor( StandardV3_2.RECORD_FORMATS ).get() );

        assertEquals( HighLimitV3_0_6.RECORD_FORMATS, findSuccessor( HighLimitV3_0_0.RECORD_FORMATS ).get() );
        assertEquals( HighLimitV3_1_0.RECORD_FORMATS, findSuccessor( HighLimitV3_0_6.RECORD_FORMATS ).get() );
        assertEquals( HighLimitV3_2_0.RECORD_FORMATS, findSuccessor( HighLimitV3_1_0.RECORD_FORMATS ).get() );
        assertEquals( HighLimit.RECORD_FORMATS, findSuccessor( HighLimitV3_2_0.RECORD_FORMATS ).get() );
    }

    private PageCache getPageCache()
    {
        return pageCacheRule.getPageCache( fs );
    }

    private void verifySelectForStore( PageCache pageCache, RecordFormats format ) throws IOException
    {
        prepareNeoStoreFile( format.storeVersion(), pageCache );
        assertSame( format, selectForStore( storeDir, pageCache, LOG ) );
    }

    private File prepareNeoStoreFile( String storeVersion, PageCache pageCache ) throws IOException
    {
        File neoStoreFile = createNeoStoreFile();
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
        return neoStoreFile;
    }

    private File createNeoStoreFile() throws IOException
    {
        fs.mkdir( storeDir );
        File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        fs.create( neoStoreFile ).close();
        return neoStoreFile;
    }

    private static Config config( String recordFormatName )
    {
        return Config.defaults( GraphDatabaseSettings.record_format, recordFormatName );
    }
}
