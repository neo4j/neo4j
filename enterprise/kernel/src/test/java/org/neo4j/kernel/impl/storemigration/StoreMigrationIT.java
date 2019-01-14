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
package org.neo4j.kernel.impl.storemigration;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class StoreMigrationIT
{
    @ClassRule
    public static final PageCacheRule pageCacheRule = new PageCacheRule();

    @ClassRule
    public static final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory( fileSystemRule.get() );

    public static final String CREATE_QUERY = readQuery();

    private static String readQuery()
    {
        InputStream in = StoreMigrationIT.class.getClassLoader().getResourceAsStream( "store-migration-data.txt" );
        String result = new BufferedReader( new InputStreamReader( in ) ).lines().collect( Collectors.joining( "\n" ) );
        try
        {
            in.close();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        return result;
    }

    protected final RecordFormats from;
    protected final RecordFormats to;

    @Parameterized.Parameters( name = "Migrate: {0}->{1}" )
    public static Iterable<Object[]> data() throws IOException
    {
        FileSystemAbstraction fs = fileSystemRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        File dir = TestDirectory.testDirectory( StoreMigrationIT.class ).prepareDirectoryForTest( "migration" );
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCache );
        VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( dir, fs ).withLogEntryReader( logEntryReader ).build();
        LogTailScanner tailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );
        List<Object[]> data = new ArrayList<>();
        ArrayList<RecordFormats> recordFormatses = new ArrayList<>();
        RecordFormatSelector.allFormats().forEach( f -> addIfNotThere( f, recordFormatses ) );
        for ( RecordFormats toFormat : recordFormatses )
        {
            UpgradableDatabase upgradableDatabase =
                    new UpgradableDatabase( storeVersionCheck, toFormat, tailScanner );
            for ( RecordFormats fromFormat : recordFormatses )
            {
                File db = new File( dir, baseDirName( toFormat, fromFormat ) );
                try
                {
                    createDb( fromFormat, db );
                    if ( !upgradableDatabase.hasCurrentVersion( db ) )
                    {
                        upgradableDatabase.checkUpgradeable( db );
                        data.add( new Object[]{fromFormat, toFormat} );
                    }
                }
                catch ( Exception e )
                {
                    //This means that the combination is not migratable.
                }
                fs.deleteRecursively( db );
            }
        }

        return data;
    }

    private static String baseDirName( RecordFormats toFormat, RecordFormats fromFormat )
    {
        return fromFormat.storeVersion() + toFormat.storeVersion();
    }

    private static void addIfNotThere( RecordFormats f, ArrayList<RecordFormats> recordFormatses )
    {
        for ( RecordFormats format : recordFormatses )
        {
            if ( format.storeVersion().equals( f.storeVersion() ) )
            {
                return;
            }
        }
        recordFormatses.add( f );
    }

    @Service.Implementation( RecordFormats.Factory.class )
    public static class Standard23Factory extends RecordFormats.Factory
    {
        public Standard23Factory()
        {
            super( StandardV2_3.STORE_VERSION );
        }

        @Override
        public RecordFormats newInstance()
        {
            return StandardV2_3.RECORD_FORMATS;
        }
    }

    @Service.Implementation( RecordFormats.Factory.class )
    public static class Standard30Factory extends RecordFormats.Factory
    {
        public Standard30Factory()
        {
            super( StandardV3_0.STORE_VERSION );
        }

        @Override
        public RecordFormats newInstance()
        {
            return StandardV3_0.RECORD_FORMATS;
        }
    }

    @Service.Implementation( RecordFormats.Factory.class )
    public static class Standard32Factory extends RecordFormats.Factory
    {
        public Standard32Factory()
        {
            super( StandardV3_2.STORE_VERSION );
        }

        @Override
        public RecordFormats newInstance()
        {
            return StandardV3_2.RECORD_FORMATS;
        }
    }

    @Service.Implementation( RecordFormats.Factory.class )
    public static class Standard34Factory extends RecordFormats.Factory
    {
        public Standard34Factory()
        {
            super( StandardV3_4.STORE_VERSION );
        }

        @Override
        public RecordFormats newInstance()
        {
            return StandardV3_4.RECORD_FORMATS;
        }
    }

    private static void createDb( RecordFormats recordFormat, File storeDir )
    {
        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormat.storeVersion() ).newGraphDatabase();
        database.shutdown();
    }

    public StoreMigrationIT( RecordFormats from, RecordFormats to )
    {
        this.from = from;
        this.to = to;
    }

    @Test
    public void shouldMigrate() throws Exception
    {
        File db = testDir.directory( baseDirName( to, from ) );
        FileSystemAbstraction fs = fileSystemRule.get();
        fs.deleteRecursively( db );
        GraphDatabaseService database = getGraphDatabaseService( db, from.storeVersion() );

        database.execute( "CREATE INDEX ON :Person(name)" );
        database.execute( "CREATE INDEX ON :Person(born)" );
        database.execute( "CREATE CONSTRAINT ON (person:Person) ASSERT exists(person.name)" );
        database.execute( CREATE_QUERY );
        long beforeNodes;
        long beforeLabels;
        long beforeKeys;
        long beforeRels;
        long beforeRelTypes;
        long beforeIndexes;
        long beforeConstraints;
        try ( Transaction ignore = database.beginTx() )
        {
            beforeNodes = database.getAllNodes().stream().count();
            beforeLabels = database.getAllLabels().stream().count();
            beforeKeys = database.getAllPropertyKeys().stream().count();
            beforeRels = database.getAllRelationships().stream().count();
            beforeRelTypes = database.getAllRelationshipTypes().stream().count();
            beforeIndexes = stream( database.schema().getIndexes() ).count();
            beforeConstraints = stream( database.schema().getConstraints() ).count();
        }
        database.shutdown();

        database = getGraphDatabaseService( db, to.storeVersion() );
        long afterNodes;
        long afterLabels;
        long afterKeys;
        long afterRels;
        long afterRelTypes;
        long afterIndexes;
        long afterConstraints;
        try ( Transaction ignore = database.beginTx() )
        {
            afterNodes = database.getAllNodes().stream().count();
            afterLabels = database.getAllLabels().stream().count();
            afterKeys = database.getAllPropertyKeys().stream().count();
            afterRels = database.getAllRelationships().stream().count();
            afterRelTypes = database.getAllRelationshipTypes().stream().count();
            afterIndexes = stream( database.schema().getIndexes() ).count();
            afterConstraints = stream( database.schema().getConstraints() ).count();
        }
        database.shutdown();

        assertEquals( beforeNodes, afterNodes ); //171
        assertEquals( beforeLabels, afterLabels ); //2
        assertEquals( beforeKeys, afterKeys ); //8
        assertEquals( beforeRels, afterRels ); //253
        assertEquals( beforeRelTypes, afterRelTypes ); //6
        assertEquals( beforeIndexes, afterIndexes ); //2
        assertEquals( beforeConstraints, afterConstraints ); //1
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService( );
        ConsistencyCheckService.Result result =
                runConsistencyChecker( db, fs, consistencyCheckService, to.storeVersion() );
        if ( !result.isSuccessful() )
        {
            fail( "Database is inconsistent after migration." );
        }
    }

    protected <T> Stream<T> stream( Iterable<T> iterable )
    {
        return StreamSupport.stream( iterable.spliterator(), false );
    }

    //This method is overridden by a blockdevice test.
    protected ConsistencyCheckService.Result runConsistencyChecker( File db, FileSystemAbstraction fs,
            ConsistencyCheckService consistencyCheckService, String storeVersion )
            throws ConsistencyCheckIncompleteException
    {
        Config config = Config.defaults( GraphDatabaseSettings.record_format, storeVersion );
        return consistencyCheckService.runFullConsistencyCheck( db, config, ProgressMonitorFactory.NONE,
                NullLogProvider.getInstance(), fs, false );
    }

    //This method is overridden by a blockdevice test.
    protected GraphDatabaseService getGraphDatabaseService( File db, String storeVersion )
    {
        return new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( db )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, storeVersion ).newGraphDatabase();
    }
}
