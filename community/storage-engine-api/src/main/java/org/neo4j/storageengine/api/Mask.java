/*
 * Copyright (c) "Neo4j"
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

package org.neo4j.storageengine.api;

import java.util.function.Consumer;

/**
 * Used to implement masking of user data fields when stringifying records, commands, and log entries.
 */
public interface Mask
{
    /**
     * Returns the string representation of the argument or a placeholder if masking.
     *
     * @param value value to stringify or mask
     * @return potentially masked string representation
     */
    String filter( Object value );

    /**
     * Calls the {@link Consumer} with the {@link StringBuilder} or writes a placeholder to it if masking.
     *
     * @param builder {@link StringBuilder} to write to
     * @param build   consumer that writes the unmasked representation
     */
    void build( StringBuilder builder, Consumer<StringBuilder> build );

    Mask NO = new Mask()
    {
        @Override
        public String filter( Object value )
        {
            return value.toString();
        }

        @Override
        public void build( StringBuilder builder, Consumer<StringBuilder> build )
        {
            build.accept( builder );
        }
    };

    Mask YES = new Mask()
    {
        private static final String PLACEHOLDER = "<MASKED>";

        @Override
        public String filter( Object value )
        {
            return PLACEHOLDER;
        }

        @Override
        public void build( StringBuilder builder, Consumer<StringBuilder> build )
        {
            builder.append( PLACEHOLDER );
        }
    };
}
