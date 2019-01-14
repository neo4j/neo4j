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
package org.neo4j.causalclustering.catchup;

import org.junit.Test;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.catchup.RequestMessageType.CORE_SNAPSHOT;
import static org.neo4j.causalclustering.catchup.RequestMessageType.INDEX_SNAPSHOT;
import static org.neo4j.causalclustering.catchup.RequestMessageType.PREPARE_STORE_COPY;
import static org.neo4j.causalclustering.catchup.RequestMessageType.STORE;
import static org.neo4j.causalclustering.catchup.RequestMessageType.STORE_FILE;
import static org.neo4j.causalclustering.catchup.RequestMessageType.STORE_ID;
import static org.neo4j.causalclustering.catchup.RequestMessageType.TX_PULL_REQUEST;
import static org.neo4j.causalclustering.catchup.RequestMessageType.UNKNOWN;

public class RequestMessageTypeTest
{
    /*
    Order should not change. New states should be added as higher values and old states should not be replaced.
     */
    @Test
    public void shouldHaveExpectedValues()
    {
        RequestMessageType[] givenStates = RequestMessageType.values();

        RequestMessageType[] exepctedStates =
                new RequestMessageType[]{TX_PULL_REQUEST, STORE, CORE_SNAPSHOT, STORE_ID, PREPARE_STORE_COPY, STORE_FILE, INDEX_SNAPSHOT, UNKNOWN};
        byte[] expectedValues = new byte[]{1, 2, 3, 4, 5, 6, 7, (byte) 404};

        assertEquals( exepctedStates.length, givenStates.length );
        assertEquals( exepctedStates.length, expectedValues.length );
        for ( int i = 0; i < givenStates.length; i++ )
        {
            RequestMessageType exepctedState = exepctedStates[i];
            RequestMessageType givenState = givenStates[i];
            assertEquals( format( "Expected %s git %s", givenState, exepctedState ), givenState.messageType(), exepctedState.messageType() );
            assertEquals( givenState.messageType(), expectedValues[i] );
        }
    }
}
