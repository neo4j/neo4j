/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.values.Value;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.index.schema.NumberValueConversion.assertValidSingleNumber;
import static org.neo4j.kernel.impl.index.schema.NumberValueConversion.toValue;

/**
 * Adds entity id because its unique key counterpart doesn't have it. The gain of having entity id in value
 * is that keys in internal tree nodes becomes smaller so that internal tree nodes can contain more keys.
 */
class UniqueNumberValue extends NumberValue
{
    static final int SIZE =
            Byte.SIZE + /* type */
            Long.SIZE + /* value bits */
            Long.SIZE;  /* entity id TODO 7 bytes could be enough. Also combine with the type byte thing*/

    long entityId;

    @Override
    public void from( long entityId, Value[] values )
    {
        extractValue( assertValidSingleNumber( values ) );
        this.entityId = entityId;
    }

    @Override
    public long getEntityId()
    {
        return entityId;
    }

    @Override
    public String toString()
    {
        return format( "type=%d,value=%s,entityId=%d", type, toValue( type, rawValueBits ), entityId );
    }
}
