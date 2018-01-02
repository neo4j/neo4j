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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import org.neo4j.kernel.impl.util.statistics.IntCounter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class RecordChangesTest
{

    private final RecordAccess.Loader<Object, Object, Object> loader = new RecordAccess.Loader<Object, Object, Object>()
    {
        @Override
        public Object newUnused( Object o, Object additionalData )
        {
            return o;
        }

        @Override
        public Object load( Object o, Object additionalData )
        {
            return o;
        }

        @Override
        public void ensureHeavy( Object o )
        {

        }

        @Override
        public Object clone( Object o )
        {
            return o.toString();
        }
    };

    @Test
    public void shouldCountChanges() throws Exception
    {
        // Given
        RecordChanges<Object, Object, Object> change = new RecordChanges<>( loader, false, new IntCounter() );

        // When
        change.getOrLoad( "K1", null ).forChangingData();
        change.getOrLoad( "K1", null ).forChangingData();
        change.getOrLoad( "K2", null ).forChangingData();
        change.getOrLoad( "K3", null ).forReadingData();

        // Then
        assertThat(change.changeSize(), equalTo(2));
    }

}
