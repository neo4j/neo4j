/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.helpers.collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.IteratorUtil.withResource;

import java.io.Closeable;
import java.util.Iterator;

import org.junit.Test;
import org.neo4j.graphdb.ResourceIterator;

public class IteratorUtilTest
{
    @Test
    public void closeAndExhaustResourceIteratorShouldBeIdempotent() throws Exception
    {
        // GIVEN
        Closeable closeable = mock( Closeable.class );
        Iterator<Integer> iterator = new ArrayIterator<Integer>( new Integer[] {1, 2} );
        ResourceIterator<Integer> resourceIterator = withResource( iterator, closeable );

        // WHEN
        while ( resourceIterator.hasNext() )
            resourceIterator.next();
        resourceIterator.close();

        // THEN
        verify( closeable, times( 1 ) ).close();
    }
}
