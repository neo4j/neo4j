/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.RandomAccessFile;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.ResourceCollection;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertNotSame;

public class PersistenceWindowPoolTest
{
    private static final TargetDirectory target = TargetDirectory.forTest( MappedPersistenceWindowTest.class );
    @Rule
    public final TargetDirectory.TestDirectory directory = target.testDirectory();
    @Rule
    public final ResourceCollection resources = new ResourceCollection();

    @Test
    public void shouldBeAbleToReAcquireReleasedWindow() throws Exception
    {
        // given
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        PersistenceWindowPool pool = new PersistenceWindowPool( "test.store", 8, file.getChannel(), 0, false, false );

        PersistenceWindow initialWindow = pool.acquire( 0, OperationType.READ );
        pool.release( initialWindow );

        // when
        PersistenceWindow window = pool.acquire( 0, OperationType.READ );

        // then
        assertNotSame( initialWindow, window );
    }
}
