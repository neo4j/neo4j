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
package org.neo4j.index.internal.gbptree;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class CrashGenerationCleanerConfigTest
{
    @Parameterized.Parameters
    public static List<Object[]> parameters()
    {
        return Arrays.asList(
                new Object[]{1},
                new Object[]{8},
                new Object[]{32},
                new Object[]{64},
                new Object[]{128}
        );
    }

    @Parameterized.Parameter
    public int availableProcessors;

    @Test
    public void everyThreadShouldHaveAtLeastOneBatchToWorkOn()
    {
        long pagesToClean = 1;
        long multiplier = 5;

        int threads = 1;
        long batchSize = 1;
        while ( threads < availableProcessors || batchSize < CrashGenerationCleaner.MAX_BATCH_SIZE )
        {
            threads = CrashGenerationCleaner.threads( pagesToClean, availableProcessors );
            batchSize = CrashGenerationCleaner.batchSize( pagesToClean, threads );
            assertTrue( "at least one batch per thread", (pagesToClean + batchSize) / batchSize >= threads );
            pagesToClean *= multiplier;
        }
    }
}
