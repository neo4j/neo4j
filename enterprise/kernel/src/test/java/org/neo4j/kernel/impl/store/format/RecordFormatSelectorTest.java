/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
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
        assertSame( StandardV2_0.RECORD_FORMATS, selectForVersion( StandardV2_0.STORE_VERSION ) );
        assertSame( StandardV2_1.RECORD_FORMATS, selectForVersion( StandardV2_1.STORE_VERSION ) );
        assertSame( StandardV2_2.RECORD_FORMATS, selectForVersion( StandardV2_2.STORE_VERSION ) );
        assertSame( StandardV2_3.RECORD_FORMATS, selectForVersion( StandardV2_3.STORE_VERSION ) );
        assertSame( StandardV3_0.RECORD_FORMATS, selectForVersion( StandardV3_0.STORE_VERSION ) );
        assertSame( HighLimitV3_0_0.RECORD_FORMATS, selectForVersion( HighLimitV3_0_0.STORE_VERSION ) );
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
        assertSame( defaultFormat(), selectForConfig( Config.empty(), LOG ) );
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
        verifySelectForStore( pageCache, StandardV2_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, StandardV2_1.RECORD_FORMATS );
        verifySelectForStore( pageCache, StandardV2_2.RECORD_FORMATS );
        verifySelectForStore( pageCache, StandardV2_3.RECORD_FORMATS );
        verifySelectForStore( pageCache, StandardV3_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimitV3_0_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimit.RECORD_FORMATS );
    }

    @Test
    public void selectForStoreWithNoStore() throws IOException
    {
        assertNull( selectForStore( storeDir, fs, getPageCache(), LOG ) );
    }

    @Test
    public void selectForStoreWithThrowingPageCache() throws IOException
    {
        createNeoStoreFile();
        PageCache pageCache = mock( PageCache.class );
        when( pageCache.pageSize() ).thenReturn( 8192 );
        when( pageCache.map( any(), anyInt(), anyVararg() ) ).thenThrow( new IOException( "No reading..." ) );
        assertNull( selectForStore( storeDir, fs, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreWithInvalidStoreVersion() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( "v9.Z.9", pageCache );
        assertNull( selectForStore( storeDir, fs, getPageCache(), LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithSameStandardConfiguredAndStoredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = config( Standard.LATEST_NAME );

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, fs, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithSameHighLimitConfiguredAndStoredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = config( HighLimit.NAME );

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, fs, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithDifferentlyConfiguredAndStoredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = config( HighLimit.NAME );

        try
        {
            selectForStoreOrConfig( config, storeDir, fs, pageCache, LOG );
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

        Config config = Config.empty();

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, fs, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithOnlyHighLimitStoredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = Config.empty();

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, fs, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithOnlyStandardConfiguredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();

        Config config = config( Standard.LATEST_NAME );

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, fs, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithOnlyHighLimitConfiguredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();

        Config config = config( HighLimit.NAME );

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, storeDir, fs, pageCache, LOG ) );
    }

    @Test
    public void selectForStoreOrConfigWithWrongConfiguredFormat() throws IOException
    {
        PageCache pageCache = getPageCache();

        Config config = config( "unknown_format" );

        try
        {
            selectForStoreOrConfig( config, storeDir, fs, pageCache, LOG );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void selectForStoreOrConfigWithoutConfiguredAndStoredFormats() throws IOException
    {
        assertSame( defaultFormat(), selectForStoreOrConfig( Config.empty(), storeDir, fs, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatWithConfiguredStandardFormat()
    {
        assertSame( Standard.LATEST_RECORD_FORMATS,
                selectNewestFormat( config( Standard.LATEST_NAME ), storeDir, fs, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatWithConfiguredHighLimitFormat()
    {
        assertSame( HighLimit.RECORD_FORMATS,
                selectNewestFormat( config( HighLimit.NAME ), storeDir, fs, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatWithWrongConfiguredFormat()
    {
        try
        {
            selectNewestFormat( config( "unknown_format" ), storeDir, fs, getPageCache(), LOG );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalArgumentException.class ) );
        }
    }

    @Test
    public void selectNewestFormatWithoutConfigAndStore()
    {
        assertSame( defaultFormat(), selectNewestFormat( Config.empty(), storeDir, fs, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatForExistingStandardStore() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = Config.empty();

        assertSame( Standard.LATEST_RECORD_FORMATS, selectNewestFormat( config, storeDir, fs, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatForExistingHighLimitStore() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = Config.empty();

        assertSame( HighLimit.RECORD_FORMATS, selectNewestFormat( config, storeDir, fs, getPageCache(), LOG ) );
    }

    @Test
    public void selectNewestFormatForExistingStoreWithLegacyFormat() throws IOException
    {
        PageCache pageCache = getPageCache();
        prepareNeoStoreFile( StandardV2_3.STORE_VERSION, pageCache );

        Config config = Config.empty();

        assertSame( defaultFormat(), selectNewestFormat( config, storeDir, fs, getPageCache(), LOG ) );
    }

    @Test
    public void findSuccessorLatestVersion() throws Exception
    {
        assertFalse( findSuccessor( defaultFormat() ).isPresent() );
    }

    @Test
    public void findSuccessorToOlderVersion() throws Exception
    {
        assertEquals( StandardV2_1.RECORD_FORMATS, findSuccessor( StandardV2_0.RECORD_FORMATS ).get() );
        assertEquals( StandardV2_2.RECORD_FORMATS, findSuccessor( StandardV2_1.RECORD_FORMATS ).get() );
        assertEquals( StandardV2_3.RECORD_FORMATS, findSuccessor( StandardV2_2.RECORD_FORMATS ).get() );
        assertEquals( StandardV3_0.RECORD_FORMATS, findSuccessor( StandardV2_3.RECORD_FORMATS ).get() );

        assertEquals( HighLimitV3_0_6.RECORD_FORMATS, findSuccessor( HighLimitV3_0_0.RECORD_FORMATS ).get() );
        assertEquals( HighLimit.RECORD_FORMATS, findSuccessor( HighLimitV3_0_6.RECORD_FORMATS ).get() );
    }

    private PageCache getPageCache()
    {
        return pageCacheRule.getPageCache( fs );
    }

    private void verifySelectForStore( PageCache pageCache, RecordFormats format ) throws IOException
    {
        prepareNeoStoreFile( format.storeVersion(), pageCache );
        assertSame( format, selectForStore( storeDir, fs, pageCache, LOG ) );
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
        return new Config( stringMap( GraphDatabaseSettings.record_format.name(), recordFormatName ) );
    }
}
