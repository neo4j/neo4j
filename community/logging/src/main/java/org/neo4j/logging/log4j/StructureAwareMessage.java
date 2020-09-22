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
package org.neo4j.logging.log4j;

import org.apache.logging.log4j.util.StringBuilderFormattable;

/**
 * Base class for supporting structured logging. This message type will be handled different by e.g. {@link Neo4jJsonLogLayout},
 * where {@link #asStructure(FieldConsumer)} will be used to generate the output. If this message is received by any other
 * layout, {@link #asString(StringBuilder)} will be called.
 */
public abstract class StructureAwareMessage implements Neo4jLogMessage, StringBuilderFormattable
{
    public interface FieldConsumer
    {
        void add( String field, String value );
        void add( String field, long value );
    }

    /**
     * Serialize the log message as a string.
     */
    public abstract void asString( StringBuilder stringBuilder );

    /**
     * Serialize a structured message.
     * @param fieldConsumer consumer of all fields that should be added to the structured message.
     */
    public abstract void asStructure( FieldConsumer fieldConsumer );

    @Override
    public String getFormattedMessage()
    {
        StringBuilder sb = new StringBuilder();
        asString( sb );
        return sb.toString();
    }

    @Override
    public void formatTo( StringBuilder buffer )
    {
        asString( buffer );
    }

    @Override
    public String getFormat()
    {
        return null;
    }

    @Override
    public Object[] getParameters()
    {
        return new Object[0];
    }

    @Override
    public Throwable getThrowable()
    {
        return null;
    }
}
