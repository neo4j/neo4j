/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.cursor;

import java.nio.channels.WritableByteChannel;

/**
 * Property item returned from property cursors
 */
public interface PropertyItem
{
    /**
     * @return the key id of the current property.
     */
    int propertyKeyId();

    /**
     * @return the value of the current property.
     */
    Object value();

    /**
     * @return the boolean value of the current property
     * @throws IllegalStateException if current property is not a boolean
     */
    boolean booleanValue();

    /**
     * @return the integer value of the current property
     * @throws IllegalStateException if current property is not an integer number
     */
    long longValue();

    /**
     * @return the real value of the current property
     * @throws IllegalStateException if current property is not a real number
     */
    double doubleValue();

    /**
     * @return the string value of the current property
     */
    String stringValue();

    /**
     * The byte array data of the current value.
     *
     * @param channel to write the data into
     */
    void byteArray( WritableByteChannel channel );
}
