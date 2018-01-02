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
package org.neo4j.kernel.impl.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.transaction.log.IOCursor;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class CursorsTest
{
    @Test
    public void shouldIterateAllCursorItems() throws Exception
    {
        List<String> strings = new ArrayList<>(  );
        strings.add("foo1");
        strings.add("foo2");
        strings.add("foo3");

        final Iterator<String> iterator = strings.iterator();
        Iterable<String> iterable = Iterables.iterable( new IOCursor<String>()
        {
            String instance;

            @Override
            public String get()
            {
                return instance;
            }

            @Override
            public boolean next() throws IOException
            {
                if (iterator.hasNext())
                {
                    instance = iterator.next();
                    return true;
                } else
                    return false;
            }

            @Override
            public void close() throws IOException
            {

            }
        } );

        List<String> result = Iterables.addAll( new ArrayList<String>(  ), iterable );

        assertThat(result, equalTo(strings));
    }
}
