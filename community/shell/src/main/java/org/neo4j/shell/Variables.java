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
package org.neo4j.shell;

import java.util.regex.Pattern;

public class Variables
{
    /**
     * The {@link org.neo4j.shell.Session} key to use to store the current node and working
     * directory (i.e. the path which the client got to it).
     */
    public static final String WORKING_DIR_KEY = "WORKING_DIR";
    public static final String CURRENT_KEY = "CURRENT_DIR";
    private static final Pattern IDENTIFIER = Pattern.compile( "^\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*$" );
    public static final String TX_COUNT = "TX_COUNT";
    /**
     * The session key for the prompt key, just like in Bash.
     */
    public static final String PROMPT_KEY = "PS1";
    /**
     * The session key for whether or not to print stack traces for exceptions.
     */
    public static final String STACKTRACES_KEY = "STACKTRACES";
    /**
     * When displaying node ids this variable is also used for getting an
     * appropriate property value from that node to display as the title.
     * This variable can contain many property keys (w/ regex) separated by
     * comma prioritized in order.
     */
    public static final String TITLE_KEYS_KEY = "TITLE_KEYS";
    /**
     * The maximum length of titles to be displayed.
     */
    public static final String TITLE_MAX_LENGTH = "TITLE_MAX_LENGTH";

    /**
     * @param key a variable name
     * @throws org.neo4j.shell.ShellException if key doesn't match a valid identifier name
     */
    public static void checkIsValidVariableName( String key ) throws
        ShellException
    {
        if (!isIdentifier( key ) ) throw new ShellException(
                key + " is no valid variable name. May only contain " +
                        "alphanumeric characters and underscores.");
    }

    public static boolean isIdentifier( String key )
    {
        return IDENTIFIER.matcher( key ).matches();
    }
}
