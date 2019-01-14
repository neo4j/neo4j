/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
