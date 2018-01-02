/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.pruning.FileCountThreshold;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class FileCountThresholdTest
{
    private final File file = mock( File.class );
    private final long version = 1l;
    private final LogFileInformation source = mock( LogFileInformation.class );

    @Test
    public void shouldReturnFalseWhenTheMaxNonEmptyLogCountIsNotReached()
    {
        // given
        final int maxNonEmptyLogCount = 2;
        final FileCountThreshold threshold = new FileCountThreshold( maxNonEmptyLogCount );

        // when
        threshold.init();
        final boolean result = threshold.reached( file, version, source );

        // then
        assertFalse( result );
    }

    @Test
    public void shouldReturnTrueWhenTheMaxNonEmptyLogCountIsReached()
    {
        // given
        final int maxNonEmptyLogCount = 2;
        final FileCountThreshold threshold = new FileCountThreshold( maxNonEmptyLogCount );

        // when
        threshold.init();
        threshold.reached( file, version, source );
        final boolean result = threshold.reached( file, version, source );

        // then
        assertTrue( result );
    }

    @Test
    public void shouldResetTheCounterWhenInitIsCalled()
    {
        // given
        final int maxNonEmptyLogCount = 2;
        final FileCountThreshold threshold = new FileCountThreshold( maxNonEmptyLogCount );

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
