/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

import org.neo4j.values.storable.ValueCategory;

/**
 * Reference to a specific index together with it's capabilities. This reference is valid until the schema of the database changes
 * (that is a create/drop of an index or constraint occurs).
 */
public interface CapableIndexReference extends IndexReference, IndexCapability
{
    String providerKey();

    String providerVersion();

    CapableIndexReference NO_INDEX = new CapableIndexReference()
    {
        @Override
        public IndexOrder[] orderCapability( ValueCategory... valueCategories )
        {
            return NO_CAPABILITY.orderCapability( valueCategories );
        }

        @Override
        public IndexValueCapability valueCapability( ValueCategory... valueCategories )
        {
            return NO_CAPABILITY.valueCapability( valueCategories );
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

        @Override
        public String providerKey()
        {
            return null;
        }

        @Override
        public String providerVersion()
        {
            return null;
        }
    };
}
