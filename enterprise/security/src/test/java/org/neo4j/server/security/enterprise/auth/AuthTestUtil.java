/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class AuthTestUtil
{
    private AuthTestUtil()
    {
    }

    public static <T> T[] with( Class<T> clazz, T[] things, T... moreThings )
    {
        return Stream.concat( Arrays.stream(things), Arrays.stream( moreThings ) ).toArray(
                size -> (T[]) Array.newInstance( clazz, size )
        );
    }

    public static String[] with( String[] things, String... moreThings )
    {
        return Stream.concat( Arrays.stream(things), Arrays.stream( moreThings ) ).toArray( String[]::new );
    }

    public static <T> List<T> listOf( T... values )
    {
        return Stream.of( values ).collect( Collectors.toList() );
    }

}
