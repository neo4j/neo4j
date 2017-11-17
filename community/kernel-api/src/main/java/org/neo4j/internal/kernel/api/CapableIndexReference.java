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
package org.neo4j.internal.kernel.api;

import org.neo4j.values.storable.ValueGroup;

/**
 * Reference to a specific index together with it's capabilities. This reference is valid until the schema of the database changes
 * (that is a create/drop of an index or constraint occurs).
 */
public interface CapableIndexReference extends IndexReference, IndexCapability
{
    CapableIndexReference NO_INDEX = new CapableIndexReference()
    {
        @Override
        public IndexOrder[] orderCapability( ValueGroup... valueGroups )
        {
            return NO_CAPABILITY.orderCapability( valueGroups );
        }

        @Override
        public IndexValueCapability valueCapability( ValueGroup... valueGroups )
        {
            return NO_CAPABILITY.valueCapability( valueGroups );
        }

        @Override
        public boolean isUnique()
        {
            return false;
        }

        @Override
        public int label()
        {
            return Token.NO_TOKEN;
        }

        @Override
        public int[] properties()
        {
            return new int[0];
        }
    };
}
