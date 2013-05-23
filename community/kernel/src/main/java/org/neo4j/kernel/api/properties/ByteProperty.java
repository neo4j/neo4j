/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

/**
 * This does not extend AbstractProperty since the JVM can take advantage of the 4 byte initial field alignment if
 * we don't extend a class that has fields.
 */
final class ByteProperty extends NumberPropertyWithin4Bytes
{
    private final byte value;
    private final long propertyKeyId;

    ByteProperty( long propertyKeyId, byte value )
    {
        this.value = value;
        this.propertyKeyId = propertyKeyId;
    }

    @Override
    public long propertyKeyId()
    {
        return propertyKeyId;
    }

    @Override
    boolean hasEqualValue( NumberPropertyWithin4Bytes that )
    {
        return value == ((ByteProperty) that).value;
    }

    @Override
    public Byte value()
    {
        return value;
    }

    @Override
    public int intValue()
    {
        return value;
    }

    @Override
    public long longValue()
    {
        return value;
    }
}
