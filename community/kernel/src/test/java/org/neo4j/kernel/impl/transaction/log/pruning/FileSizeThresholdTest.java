/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.io.File;

import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.pruning.FileSizeThreshold;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileSizeThresholdTest
{
    private FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final LogFileInformation source = mock( LogFileInformation.class );
    private final File file = mock( File.class );
    private final long version = 1;

    @Test
    public void shouldReturnFalseWhenFileSizeIsLowerThanMaxSize()
    {
        // given
        final long maxSize = 10;
        final FileSizeThreshold threshold = new FileSizeThreshold( fs, maxSize );

        when( fs.getFileSize( file ) ).thenReturn( 5l );

        // when
        threshold.init();
        final boolean result = threshold.reached( file, version, source );

        // then
        assertFalse( result );
    }

    @Test
    public void shouldReturnTrueWhenASingleFileSizeIsGreaterOrEqualThanMaxSize()
    {
        // given
        final long maxSize = 10;
        final FileSizeThreshold threshold = new FileSizeThreshold( fs, maxSize );

        when( fs.getFileSize( file ) ).thenReturn( 10l );

        // when
        threshold.init();
        final boolean result = threshold.reached( file, version, source );

        // then
        assertTrue( result );
    }

    @Test
    public void shouldSumSizeWhenCalledMultipleTimes()
    {
        // given
        final long maxSize = 10;
        final FileSizeThreshold threshold = new FileSizeThreshold( fs, maxSize );

        when( fs.getFileSize( file ) ).thenReturn( 5l );

        // when
        threshold.init();
        threshold.reached( file, version, source );
        final boolean result = threshold.reached( file, version, source );

        // then
        assertTrue( result );
    }

    @Test
    public void shouldForgetPreviousValuesAfterAInitCall()
    {
        // given
        final long maxSize = 10;
        final FileSizeThreshold threshold = new FileSizeThreshold( fs, maxSize );

        when( fs.getFileSize( file ) ).thenReturn( 5l );

        // when
        threshold.init();
        threshold.reached( file, version, source );
        threshold.reached( file, version, source );
        threshold.init();
        final boolean result = threshold.reached( file, version, source );

        // then
        assertFalse( result );
    }
}
