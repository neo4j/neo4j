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
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.E_LISTING_STORE;
import static org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status.SUCCESS;

public class PrepareStoreCopyResponseTest
{
    /*
    Order should not change. New statuses should be added as higher ordinal and old statuses should not be replaced.
     */
    @Test
    public void shouldMaintainOrderOfStatuses()
    {
        PrepareStoreCopyResponse.Status[] givenValues = PrepareStoreCopyResponse.Status.values();
        PrepareStoreCopyResponse.Status[] expectedValues = new PrepareStoreCopyResponse.Status[]{SUCCESS, E_STORE_ID_MISMATCH, E_LISTING_STORE};

        assertArrayEquals( givenValues, expectedValues );
    }
}
