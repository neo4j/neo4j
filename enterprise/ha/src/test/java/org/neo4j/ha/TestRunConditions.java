/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ha;

import org.apache.commons.lang3.SystemUtils;

/**
 * This class is expected to contain conditions for various tests, controlling whether they run or not. It is supposed
 * to be used from {@link org.junit.Assume} statements in tests to save adding and removing {@link org.junit.Ignore}
 * annotations. Static methods and fields that check for environmental or other conditions should be placed here as
 * a way to have a central point of control for them.
 */
public class TestRunConditions
{
    /**
     * Largest cluster size which can run without (many) problems in a typical windows build
     */
    private static final int MAX_WINDOWS_CLUSTER_SIZE = 3;

    /**
     * Largest cluster size which can run without (many) problems on any plaform
     */
    private static final int MAX_CLUSTER_SIZE = 5;

    private TestRunConditions()
    {
    }

    public static boolean shouldRunAtClusterSize( int clusterSize )
    {
        if ( clusterSize <= MAX_WINDOWS_CLUSTER_SIZE )
        {
            // If it's less than or equal to the minimum allowed size regardless of platform
            return true;
        }
        if ( clusterSize > MAX_WINDOWS_CLUSTER_SIZE && clusterSize <= MAX_CLUSTER_SIZE && !SystemUtils.IS_OS_WINDOWS )
        {
            // If it's below the maximum cluster size but not on windows
            return true;
        }
        // here it's either (above max size) or (below max size and above max windows size and on windows)
        return false;
    }
}
