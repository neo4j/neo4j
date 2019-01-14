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
package org.neo4j.commandline.arguments;

import org.neo4j.helpers.Args;

public class OptionalPositionalArgument implements PositionalArgument
{
    protected final String value;
    final int position;

    public OptionalPositionalArgument( int position, String value )
    {
        this.position = position;
        this.value = value;
    }

    @Override
    public int position()
    {
        return position;
    }

    @Override
    public String usage()
    {
        return String.format( "[<%s>]", value );
    }

    @Override
    public String parse( Args parsedArgs )
    {
        return parsedArgs.orphans().get( position );
    }
}
