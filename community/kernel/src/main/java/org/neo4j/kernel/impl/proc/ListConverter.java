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
package org.neo4j.kernel.impl.proc;

import java.lang.reflect.Type;
import java.util.function.Function;

import org.neo4j.kernel.api.proc.Neo4jTypes;

import static org.neo4j.kernel.impl.proc.Neo4jValue.ntList;
import static org.neo4j.kernel.impl.proc.ParseUtil.parseList;


public class ListConverter implements Function<String,Neo4jValue>
{
    private final Type type;
    private final Neo4jTypes.AnyType neoType;

    public ListConverter( Type type, Neo4jTypes.AnyType neoType )
    {
        this.type = type;
        this.neoType = neoType;
    }

    @Override
    public Neo4jValue apply( String s )
    {
        String value = s.trim();
        if ( value.equalsIgnoreCase( "null" ) )
        {
            return ntList( null, neoType );
        }
        else
        {
            return ntList( parseList( value, type ), neoType );
        }
    }
}
