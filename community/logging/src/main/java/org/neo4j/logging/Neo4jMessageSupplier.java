/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.logging;

import org.apache.logging.log4j.util.MessageSupplier;

public interface Neo4jMessageSupplier extends MessageSupplier {
    @Override
    Neo4jLogMessage get();

    /**
     * Helper function to create a {@link Neo4jLogMessage} to wrap in a {@link Neo4jMessageSupplier}. Example usage:
     * <pre>
     *     {@code log.info( () -> Neo4jMessageSupplier.forMessage( "My log message %s", computeBigStringArg() ) ); }
     * </pre>
     */
    static Neo4jLogMessage forMessage(String format, Object... args) {
        return new Neo4jLogMessage() {
            private final String formattedMessage = String.format(format, args);

            @Override
            public String getFormattedMessage() {
                return formattedMessage;
            }

            @Override
            public String getFormat() {
                return format;
            }

            @Override
            public Object[] getParameters() {
                return args;
            }

            @Override
            public Throwable getThrowable() {
                return null;
            }
        };
    }
}
