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
package org.neo4j.kernel.impl.transaction;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class LogPositionMarkerTest
{
    @Test
    public void shouldReturnUnspecifiedIfNothingHasBeenMarked()
    {
        // given
        final LogPositionMarker marker = new LogPositionMarker();

        // when
        final LogPosition logPosition = marker.newPosition();

        // given
        assertEquals(LogPosition.UNSPECIFIED, logPosition);
    }

    @Test
    public void shouldReturnTheMarkedPosition()
    {
        // given
        final LogPositionMarker marker = new LogPositionMarker();

        // when
        marker.mark( 1, 2 );
        final LogPosition logPosition = marker.newPosition();

        // given
        assertEquals(new LogPosition( 1, 2 ), logPosition);
    }

    @Test
    public void shouldReturnUnspecifiedWhenAskedTo()
    {
        // given
        final LogPositionMarker marker = new LogPositionMarker();

        // when
        marker.mark( 1, 2 );
        marker.unspecified();
        final LogPosition logPosition = marker.newPosition();

        // given
        assertEquals(LogPosition.UNSPECIFIED, logPosition);
    }
}
