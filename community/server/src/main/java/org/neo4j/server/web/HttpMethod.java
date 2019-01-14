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
package org.neo4j.server.web;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import static java.util.Collections.unmodifiableMap;

public enum HttpMethod
{
    OPTIONS,
    GET,
    HEAD,
    POST,
    PUT,
    PATCH,
    DELETE,
    TRACE,
    CONNECT;

    private static final Map<String,HttpMethod> methodsByName = indexMethodsByName();

    @Nullable
    public static HttpMethod valueOfOrNull( String name )
    {
        return methodsByName.get( name );
    }

    private static Map<String,HttpMethod> indexMethodsByName()
    {
        HttpMethod[] methods = HttpMethod.values();
        Map<String,HttpMethod> result = new HashMap<>( methods.length * 2 );
        for ( HttpMethod method : methods )
        {
            result.put( method.name(), method );
        }
        return unmodifiableMap( result );
    }
}
