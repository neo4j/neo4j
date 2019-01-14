/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.core;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IteratingPropertyReceiverTest
{
    @Test
    public void shouldAcceptAndThenIterateOverProperties()
    {
        // GIVEN
        IteratingPropertyReceiver<StorageProperty> receiver = new IteratingPropertyReceiver<>();
        int propertyCount = 100;
        for ( int i = 0; i < propertyCount; i++ )
        {
            receiver.receive( new PropertyKeyValue( 1, Values.of( i ) ), 5 );
        }

        // THEN
        int count = 0;
        while ( receiver.hasNext() )
        {
            StorageProperty property = receiver.next();
            assertEquals( Values.of( count++ ), property.value() );
        }
        assertFalse( receiver.hasNext() );
        assertEquals( propertyCount, count );
    }
}
