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
package org.neo4j.ext.udc.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.udc.UsageDataKeys.Features.bolt;

@ExtendWith( TestDirectoryExtension.class )
class DefaultUdcInformationCollectorTest
{
    @Inject
    private TestDirectory testDirectory;

    private final UsageData usageData = new UsageData( mock( JobScheduler.class ) );

    private final DataSourceManager dataSourceManager = new DataSourceManager( Config.defaults() );
    private final NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );
    private final DefaultUdcInformationCollector collector = new DefaultUdcInformationCollector( Config.defaults(), dataSourceManager, usageData );
    private final DefaultFileSystemAbstraction fileSystem = mock( DefaultFileSystemAbstraction.class );

    @BeforeEach
    void setUp()
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( new StubIdGeneratorFactory() );
        dependencies.satisfyDependencies( fileSystem );
        when( dataSource.getDependencyResolver() ).thenReturn( dependencies );
        when( dataSource.getDatabaseLayout() ).thenReturn( DatabaseLayout.of( new File( "database" ) ) );
        when( dataSource.getStoreId() ).thenReturn( StoreId.DEFAULT );

        dataSourceManager.start();
        dataSourceManager.register( dataSource );
    }

    @Test
    void shouldIncludeTheMacAddress()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.MAC ) );
    }

    @Test
    void shouldIncludeTheNumberOfProcessors()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.NUM_PROCESSORS ) );
    }

    @Test
    void shouldIncludeTotalMemorySize()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.TOTAL_MEMORY ) );
    }

    @Test
    void shouldIncludeHeapSize()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.HEAP_SIZE ) );
    }

    @Test
    void shouldIncludeNodeIdsInUse()
    {
        assertEquals( "100", collector.getUdcParams().get( UdcConstants.NODE_IDS_IN_USE ) );
    }

    @Test
    void shouldIncludeRelationshipIdsInUse()
    {
        assertEquals( "200", collector.getUdcParams().get( UdcConstants.RELATIONSHIP_IDS_IN_USE ) );
    }

    @Test
    void shouldIncludePropertyIdsInUse()
    {
        assertEquals( "400", collector.getUdcParams().get( UdcConstants.PROPERTY_IDS_IN_USE ) );
    }

    @Test
    void shouldIncludeLabelIdsInUse()
    {
        assertEquals( "300", collector.getUdcParams().get( UdcConstants.LABEL_IDS_IN_USE ) );
    }

    @Test
    void shouldIncludeVersionEditionAndMode()
    {
        // Given
        usageData.set( UsageDataKeys.version, "1.2.3" );
        usageData.set( UsageDataKeys.edition, Edition.enterprise );
        usageData.set( UsageDataKeys.operationalMode, OperationalMode.ha );

        // When & Then
        assertEquals( "1.2.3", collector.getUdcParams().get( UdcConstants.VERSION ) );
        assertEquals( "enterprise", collector.getUdcParams().get( UdcConstants.EDITION ) );
        assertEquals( "ha", collector.getUdcParams().get( UdcConstants.DATABASE_MODE ) );
    }

    @Test
    void shouldIncludeRecentClientNames()
    {
        // Given
        usageData.get( UsageDataKeys.clientNames ).add( "SteveBrookClient/1.0" );
        usageData.get( UsageDataKeys.clientNames ).add( "MayorClient/1.0" );

        // When & Then
        String userAgents = collector.getUdcParams().get( UdcConstants.USER_AGENTS );
        if ( !(userAgents.equals( "SteveBrookClient/1.0,MayorClient/1.0" ) ||
                userAgents.equals( "MayorClient/1.0,SteveBrookClient/1.0" )) )
        {
            fail( "Expected \"SteveBrookClient/1.0,MayorClient/1.0\" or \"MayorClient/1.0,SteveBrookClient/1.0\", " +
                    "got \"" + userAgents + "\"" );
        }
    }

    @Test
    void shouldIncludePopularFeatures()
    {
        // Given
        usageData.get( UsageDataKeys.features ).flag( bolt );

        // When & Then
        assertEquals( "1000", collector.getUdcParams().get( UdcConstants.FEATURES ) );
    }

    @Test
    void shouldReportStoreSizes() throws Throwable
    {
        UdcInformationCollector collector = new DefaultUdcInformationCollector( Config.defaults(), dataSourceManager, usageData );

        when( fileSystem.getFileSize( Mockito.any() ) ).thenReturn( 152L );
        Map<String, String> udcParams = collector.getUdcParams();

        assertThat( udcParams.get( "storesize" ), is( "152" ) );
    }

    private static Set<StoreFileMetadata> toMeta( File... files )
    {
        return Arrays.stream( files )
                .map( file -> new StoreFileMetadata( file, RecordFormat.NO_RECORD_SIZE ) )
                .collect( Collectors.toCollection( HashSet::new ) );
    }

    private static class StubIdGeneratorFactory implements IdGeneratorFactory
    {
        private final Map<IdType, Long> idsInUse = new HashMap<>();

        StubIdGeneratorFactory()
        {
            idsInUse.put( IdType.NODE, 100L );
            idsInUse.put( IdType.RELATIONSHIP, 200L );
            idsInUse.put( IdType.LABEL_TOKEN, 300L );
            idsInUse.put( IdType.PROPERTY, 400L );
        }

        @Override
        public IdGenerator open( File filename, IdType idType, LongSupplier highId, long maxId )
        {
            return open( filename, 0, idType, highId, maxId );
        }

        @Override
        public IdGenerator open( File fileName, int grabSize, IdType idType, LongSupplier highId, long maxId )
        {
            return get( idType );
        }

        @Override
        public void create( File fileName, long highId, boolean throwIfFileExists )
        {   // Ignore
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return new StubIdGenerator( idsInUse.get( idType ) );
        }
    }

    private static class StubIdGenerator implements IdGenerator
    {
        private final long numberOfIdsInUse;

        private StubIdGenerator( long numberOfIdsInUse )
        {
            this.numberOfIdsInUse = numberOfIdsInUse;
        }

        @Override
        public long nextId()
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public void setHighId( long id )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public long getHighId()
        {
            return 0;
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return 0;
        }

        @Override
        public void freeId( long id )
        {   // Ignore
        }

        @Override
        public void close()
        {   // Ignore
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return numberOfIdsInUse;
        }

        @Override
        public long getDefragCount()
        {
            return 0;
        }

        @Override
        public void delete()
        {   // Ignore
        }
    }
}
