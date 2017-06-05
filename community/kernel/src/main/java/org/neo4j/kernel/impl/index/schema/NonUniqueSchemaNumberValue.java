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

import static java.lang.String.format;

import static org.neo4j.kernel.impl.index.schema.SchemaNumberValueConversion.assertValidSingleNumberPropertyValue;
import static org.neo4j.kernel.impl.index.schema.SchemaNumberValueConversion.toValue;

/**
 * Relies on its key counterpart to supply entity id, since the key needs entity id anyway.
 */
class NonUniqueSchemaNumberValue extends SchemaNumberValue
{
    static final int SIZE =
            Byte.SIZE + /* type */
            Long.SIZE;  /* value bits */

    @Override
    public void from( long entityId, Object[] values )
    {
        assertValidSingleNumberPropertyValue( values );
        extractValue( (Number) values[0] );
    }

    @Override
    public long getEntityId()
    {
        throw new UnsupportedOperationException( "entity id should be retrieved from key for non-unique index" );
    }

    @Override
    public String toString()
    {
        return format( "type=%d,value=%s", type, toValue( type, rawValueBits ) );
    }
}
