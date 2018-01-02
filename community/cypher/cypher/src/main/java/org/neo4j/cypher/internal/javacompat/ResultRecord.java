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
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.cypher.result.QueryResult;
import org.neo4j.values.AnyValue;

public class ResultRecord implements QueryResult.Record
{
    private final AnyValue[] fields;

    //NOTE do not remove, used from generated code
    public ResultRecord( int size )
    {
        this.fields = new AnyValue[size];
    }

    public ResultRecord( AnyValue[] fields )
    {
        this.fields = fields;
    }

    public void set( int i, AnyValue value )
    {
        assert value != null;
        assert i >= 0 && i < fields.length;

        fields[i] = value;
    }

    @Override
    public AnyValue[] fields()
    {
        return fields;
    }
}
