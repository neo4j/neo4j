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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.index.SetInitialStateInNativeIndex;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.OtherThreadRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_FAILED;
import static org.neo4j.test.TestLabels.LABEL_ONE;

@RunWith( Parameterized.class )
@Ignore
public class IndexCleanupIT
{
    @Parameterized.Parameters( name = "{index} {0}" )
    public static Collection<org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex> schemaIndexes()
    {
        return Arrays.asList( GraphDatabaseSettings.SchemaIndex.values() );
    }

    @Parameterized.Parameter
    public GraphDatabaseSettings.SchemaIndex schemaIndex;

    private static final String propertyKey = "key";

    private RandomRule random = new RandomRule();
    private FileSystemRule fs = new DefaultFileSystemRule();
    private TestDirectory directory = TestDirectory.testDirectory( fs );
    private DbmsRule db = new EmbeddedDbmsRule( directory ).startLazily();
    private OtherThreadRule<Void> t2 = new OtherThreadRule( "T2" );

    @Rule
    public RuleChain rules = RuleChain.outerRule( random ).around( fs ).around( directory ).around( db );

    @Test
    public void mustClearIndexDirectoryOnDropWhileOnline()
    {
        configureDb( schemaIndex );
        createIndex( db, true );

        File[] providerDirectories = providerDirectories( fs, db );
        for ( File providerDirectory : providerDirectories )
        {
            assertTrue( fs.listFiles( providerDirectory ).length > 0, "expected there to be at least one index per existing provider map" );
        }

        dropAllIndexes();

        assertNoIndexFilesExisting( providerDirectories );
    }

    @Test
    public void mustClearIndexDirectoryOnDropWhileFailed() throws IOException
    {
        configureDb( schemaIndex );
        createIndex( db, true );
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( schemaIndex.providerKey(), schemaIndex.providerVersion() );
        SetInitialStateInNativeIndex setInitialStateInNativeIndex = new SetInitialStateInNativeIndex( BYTE_FAILED, providerDescriptor );
        db.restartDatabase( setInitialStateInNativeIndex );
        // Index should be failed at this point

        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : db.schema().getIndexes() )
            {
                Schema.IndexState indexState = db.schema().getIndexState( index );
                assertEquals( Schema.IndexState.FAILED, indexState, "expected index state to be " + Schema.IndexState.FAILED );
            }
            tx.commit();
        }

        // when
        dropAllIndexes();

        // then
        assertNoIndexFilesExisting( providerDirectories( fs, db ) );
    }

    @Test
    public void mustClearIndexDirectoryOnDropWhilePopulating() throws InterruptedException
    {
        // given
        Barrier.Control midPopulation = new Barrier.Control();
        IndexingService.MonitorAdapter trappingMonitor = new IndexingService.MonitorAdapter()
        {
            @Override
            public void indexPopulationScanStarting()
            {
                midPopulation.reached();
            }
        };
        configureDb( schemaIndex );
        Monitors monitors = db.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( Monitors.class );
        monitors.addMonitorListener( trappingMonitor );
        createIndex( db, false );

        midPopulation.await();
        File[] providerDirectories = providerDirectories( fs, db );
        for ( File providerDirectory : providerDirectories )
        {
            assertTrue( fs.listFiles( providerDirectory ).length > 0, "expected there to be at least one index per existing provider map" );
        }

        // when
        dropAllIndexes();
        midPopulation.release();

        assertNoIndexFilesExisting( providerDirectories );
    }

    private void assertNoIndexFilesExisting( File[] providerDirectories )
    {
        for ( File providerDirectory : providerDirectories )
        {
            assertEquals( 0, fs.listFiles( providerDirectory ).length, "expected there to be no index files" );
        }
    }

    private void dropAllIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().getIndexes().forEach( IndexDefinition::drop );
            tx.commit();
        }
    }

    private void configureDb( GraphDatabaseSettings.SchemaIndex schemaIndex )
    {
        db.withSetting( GraphDatabaseSettings.default_schema_provider, schemaIndex.providerName() );
    }

    private void createIndex( GraphDatabaseService db, boolean awaitOnline )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition indexDefinition = db.schema().indexFor( LABEL_ONE ).on( propertyKey ).create();
            tx.commit();
        }
        if ( awaitOnline )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                tx.commit();
            }
        }
    }

    private File[] providerDirectories( FileSystemAbstraction fs, DbmsRule db )
    {
        DatabaseLayout databaseLayout = db.databaseLayout();
        File dbDir = databaseLayout.databaseDirectory();
        File schemaDir = new File( dbDir, "schema" );
        File indexDir = new File( schemaDir, "index" );
        return fs.listFiles( indexDir );
    }
}
