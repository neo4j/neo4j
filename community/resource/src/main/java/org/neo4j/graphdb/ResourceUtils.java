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
package org.neo4j.graphdb;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ResourceUtils
{
    /**
     * @param resources {@link Iterable} over resources to close.
     */
    public static <T extends Resource> void closeAll( Iterable<T> resources )
    {
        closeAll( StreamSupport.stream( resources.spliterator(), false ) );
    }

    /**
     * @param resources Array of resources to close.
     */
    @SafeVarargs
    public static <T extends Resource> void closeAll( T... resources )
    {
        closeAll( Arrays.stream( resources ) );
    }

    /**
     * Close all resources. Does NOT guarantee all being closed in case of unchecked exception.
     *
     * @param resources Stream of resources to close.
     */
    public static <T extends Resource> void closeAll( Stream<T> resources )
    {
        resources.filter( Objects::nonNull ).forEach( Resource::close );
    }
}
