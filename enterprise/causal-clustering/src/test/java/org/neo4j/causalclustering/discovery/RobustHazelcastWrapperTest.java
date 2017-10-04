/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.discovery;

import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RobustHazelcastWrapperTest
{
    @Test
    public void shouldReconnectIfHazelcastConnectionInvalidated() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastInstance hzInstance = mock( HazelcastInstance.class );

        when( connector.connectToHazelcast() ).thenReturn( hzInstance );

        RobustHazelcastWrapper hzWrapper = new RobustHazelcastWrapper( connector );

        // when
        hzWrapper.perform( hz ->
        { /* nothing*/ } );

        // then
        verify( connector, times( 1 ) ).connectToHazelcast();

        // then
        try
        {
            hzWrapper.perform( hz ->
            {
                throw new com.hazelcast.core.HazelcastInstanceNotActiveException();
            } );
            fail();
        }
        catch ( HazelcastInstanceNotActiveException e )
        {
            // expected
        }

        // when
        hzWrapper.perform( hz ->
        { /* nothing*/ } );
        hzWrapper.perform( hz ->
        { /* nothing*/ } );

        // then
        verify( connector, times( 2 ) ).connectToHazelcast();
    }
}
