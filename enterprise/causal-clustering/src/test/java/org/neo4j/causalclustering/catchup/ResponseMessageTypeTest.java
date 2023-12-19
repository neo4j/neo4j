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
package org.neo4j.causalclustering.catchup;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.catchup.ResponseMessageType.CORE_SNAPSHOT;
import static org.neo4j.causalclustering.catchup.ResponseMessageType.FILE;
import static org.neo4j.causalclustering.catchup.ResponseMessageType.INDEX_SNAPSHOT_RESPONSE;
import static org.neo4j.causalclustering.catchup.ResponseMessageType.PREPARE_STORE_COPY_RESPONSE;
import static org.neo4j.causalclustering.catchup.ResponseMessageType.STORE_COPY_FINISHED;
import static org.neo4j.causalclustering.catchup.ResponseMessageType.STORE_ID;
import static org.neo4j.causalclustering.catchup.ResponseMessageType.TX;
import static org.neo4j.causalclustering.catchup.ResponseMessageType.TX_STREAM_FINISHED;
import static org.neo4j.causalclustering.catchup.ResponseMessageType.UNKNOWN;

public class ResponseMessageTypeTest
{
    /*
    Order should not change. New states should be added as higher values and old states should not be replaced.
     */
    @Test
    public void shouldHaveExpectedValues()
    {
        ResponseMessageType[] givenStates = ResponseMessageType.values();

        ResponseMessageType[] exepctedStates =
                new ResponseMessageType[]{TX, STORE_ID, FILE, STORE_COPY_FINISHED, CORE_SNAPSHOT, TX_STREAM_FINISHED, PREPARE_STORE_COPY_RESPONSE,
                        INDEX_SNAPSHOT_RESPONSE, UNKNOWN};

        byte[] expectedValues = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, (byte) 200};

        assertEquals( exepctedStates.length, givenStates.length );
        assertEquals( givenStates.length, expectedValues.length );
        for ( int i = 0; i < givenStates.length; i++ )
        {
            assertEquals( givenStates[i].messageType(), exepctedStates[i].messageType() );
            assertEquals( givenStates[i].messageType(), expectedValues[i] );
        }
    }
}
