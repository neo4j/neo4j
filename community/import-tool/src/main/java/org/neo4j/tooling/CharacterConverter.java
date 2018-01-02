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
package org.neo4j.tooling;

import org.neo4j.function.Function;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;

/**
 * Converts a string expression into a character to be used as delimiter, array delimiter, or quote character. Can be
 * normal characters as well as for example: '\t', '\123', and "TAB".
 */
class CharacterConverter implements Function<String,Character>
{

    @Override
    public Character apply( String value ) throws RuntimeException
    {
        // Parse "raw" ASCII character style characters:
        // - \123 --> character with id 123
        // - \t   --> tab character
        if ( value.startsWith( "\\" ) && value.length() > 1 )
        {
            String raw = value.substring( 1 );
            try
            {
                return (char) Integer.parseInt( raw );
            }
            catch ( NumberFormatException e )
            {
                if ( raw.equals( "t" ) )
                {
                    return Configuration.TABS.delimiter();
                }
            }
        }
        // hard coded TAB --> tab character
        else if ( value.equals( "TAB" ) )
        {
            return Configuration.TABS.delimiter();
        }
        else if ( value.length() == 1 )
        {
            return value.charAt( 0 );
        }

        throw new IllegalArgumentException( "Unsupported character '" + value + "'" );
    }
}
