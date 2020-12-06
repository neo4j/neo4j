/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.shell.test;

public class Util
{
    public static String[] asArray( String... arguments )
    {
        return arguments;
    }

    /**
     * Generate the control code for the specified character. For example, give this method 'C', and it will return the code for `Ctrl-C`, which you can append
     * to an inputbuffer for example, in order to simulate the  user pressing Ctrl-C.
     *
     * @param let character to generate code for, must be between A and Z
     * @return control code for given character
     */
    public static char ctrl( final char let )
    {
        if ( let < 'A' || let > 'Z' )
        {
            throw new IllegalArgumentException( "Cannot generate CTRL code for "
                                                + "char '" + let + "' (" + ((int) let) + ")" );
        }

        int result = ((int) let) - 'A' + 1;
        return (char) result;
    }

    public static class NotImplementedYetException extends RuntimeException
    {
        public NotImplementedYetException( String message )
        {
            super( message );
        }
    }
}
