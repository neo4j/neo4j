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
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_UNKNOWN;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.values;

public class StoreCopyFinishedResponseTest
{
    /*
    Order should not change. New statuses should be added as higher ordinal and old statuses should not be replaced.
     */
    @Test
    public void shouldMaintainOrderOfStatuses()
    {
        StoreCopyFinishedResponse.Status[] givenValues = values();
        StoreCopyFinishedResponse.Status[] expectedValues = new StoreCopyFinishedResponse.Status[]{SUCCESS, E_STORE_ID_MISMATCH, E_TOO_FAR_BEHIND, E_UNKNOWN};

        assertArrayEquals( givenValues, expectedValues );
    }
}
