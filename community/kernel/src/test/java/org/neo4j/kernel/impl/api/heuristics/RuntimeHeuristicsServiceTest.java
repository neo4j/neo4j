/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.heuristics;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.test.TargetDirectory;

import java.io.File;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.kernel.impl.api.heuristics.HeuristicsTestSupport.generateStore;

public class RuntimeHeuristicsServiceTest
{
    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.cleanTestDirForTest( getClass() );

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();


    @Test
    public void shouldSerializeAndDeserialize() throws Exception
    {
        // Given
        StoreReadLayer store = generateStore();
        HeuristicsCollectedData collectedData = new HeuristicsCollectedData();
        RuntimeHeuristicsService service = new RuntimeHeuristicsService( collectedData, store, null );
        HeuristicsCollector collector = new HeuristicsCollector( store, collectedData );
        collector.run();

        // When
        service.save( fs, new File( dir.directory(), "somefile" ) );

        // Then
        HeuristicsCollectedData expected = (HeuristicsCollectedData) RuntimeHeuristicsService.load( fs,
                new File( dir.directory(), "somefile" ), store, null ).heuristics();
        assertThat( expected, equalTo( collector.collectedData() ) );
    }

    @Test
    public void shouldSerializeTwiceAndDeserialize() throws Exception
    {
        // Given
        StoreReadLayer store = generateStore();
        HeuristicsCollectedData collectedData = new HeuristicsCollectedData();
        RuntimeHeuristicsService service = new RuntimeHeuristicsService( collectedData, store, null );
        HeuristicsCollector collector = new HeuristicsCollector( store, collectedData );
        collector.run();

        // When
        service.save( fs, new File( dir.directory(), "somefile" ) );
        service.save( fs, new File( dir.directory(), "somefile" ) );

        // Then
        HeuristicsCollectedData expected = (HeuristicsCollectedData) RuntimeHeuristicsService.load( fs,
                new File( dir.directory(), "somefile" ), store, null ).heuristics();
        assertThat( expected, equalTo( collector.collectedData() ) );
    }
}
