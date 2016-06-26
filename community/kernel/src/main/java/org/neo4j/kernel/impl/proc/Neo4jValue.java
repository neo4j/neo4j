/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.api.proc.Neo4jTypes;

public class Neo4jValue
{
    private final Object value;
    private final Neo4jTypes.AnyType type;

    public Neo4jValue( Object value, Neo4jTypes.AnyType type )
    {
        this.value = value;
        this.type = type;
    }

    public Object value()
    {
        return value;
    }

    public Neo4jTypes.AnyType neo4jType()
    {
        return type;
    }

    public static  Neo4jValue ntString(String value)
    {
        return new Neo4jValue( value, Neo4jTypes.NTString );
    }

    public static  Neo4jValue ntInteger(long value)
    {
        return new Neo4jValue( value, Neo4jTypes.NTInteger );
    }

    public static  Neo4jValue ntFloat(double value)
    {
        return new Neo4jValue( value, Neo4jTypes.NTFloat );
    }

    public static  Neo4jValue ntBoolean(boolean value)
    {
        return new Neo4jValue( value, Neo4jTypes.NTBoolean );
    }
}
