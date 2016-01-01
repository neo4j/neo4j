/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.statistics;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.kernel.impl.api.statistics.HeuristicsTestSupport.generateStore;

public class SamplingStatisticsServiceTest
{
    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();


    @Test
    public void shouldSerializeAndDeserialize() throws Exception
    {
        // Given
        StoreReadLayer store = generateStore();
        StatisticsCollectedData collectedData = new StatisticsCollectedData();
        SamplingStatisticsService service = new SamplingStatisticsService( collectedData, store, null );
        StatisticsCollector collector = new StatisticsCollector( store, collectedData );
        collector.run();

        // When
        service.save( fs, new File( dir.directory(), "somefile" ) );

        // Then
        StatisticsCollectedData expected = (StatisticsCollectedData) SamplingStatisticsService.load( fs,
                new File( dir.directory(), "somefile" ), store, null ).statistics();
        assertThat( expected, equalTo( collector.collectedData() ) );
    }

    @Test
    public void shouldSerializeTwiceAndDeserialize() throws Exception
    {
        // Given
        StoreReadLayer store = generateStore();
        StatisticsCollectedData collectedData = new StatisticsCollectedData();
        SamplingStatisticsService service = new SamplingStatisticsService( collectedData, store, null );
        StatisticsCollector collector = new StatisticsCollector( store, collectedData );
        collector.run();

        // When
        service.save( fs, new File( dir.directory(), "somefile" ) );
        service.save( fs, new File( dir.directory(), "somefile" ) );

        // Then
        StatisticsCollectedData expected = (StatisticsCollectedData) SamplingStatisticsService.load( fs,
                new File( dir.directory(), "somefile" ), store, null ).statistics();
        assertThat( expected, equalTo( collector.collectedData() ) );
    }
}
