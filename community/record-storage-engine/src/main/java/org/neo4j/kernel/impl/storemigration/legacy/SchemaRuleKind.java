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
package org.neo4j.kernel.impl.storemigration.legacy;

import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;

public enum SchemaRuleKind
{
    INDEX_RULE,
    CONSTRAINT_INDEX_RULE,
    UNIQUENESS_CONSTRAINT,
    NODE_PROPERTY_EXISTENCE_CONSTRAINT,
    RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT;

    private static final SchemaRuleKind[] ALL = values();

    public static SchemaRuleKind forId( byte id ) throws MalformedSchemaRuleException
    {
        if ( id >= 1 && id <= ALL.length )
        {
            return values()[id - 1];
        }
        throw new MalformedSchemaRuleException( null, "Unknown kind id %d", id );
    }
}
