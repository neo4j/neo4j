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
package org.neo4j.kernel.api.schema_new.index;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;

public interface NewIndexDescriptor
{
    enum Type { GENERAL, UNIQUE }

    Type type();

    /**
     * This method currently returns the specific LabelSchemaDescriptor, as we do not support indexes on relations.
     * When we do, consider down-typing this to a SchemaDescriptor.
     */
    LabelSchemaDescriptor schema();

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of what this index indexes.
     */
    String userDescription( TokenNameLookup tokenNameLookup );

    /**
     * Checks whether a constraint descriptor Supplier supplies this constraint descriptor.
     * @param supplier supplier to get a constraint descriptor from
     * @return true if the supplied constraint descriptor equals this constraint descriptor
     */
    default boolean isSame( Supplier supplier )
    {
        return this.equals( supplier.getNewIndexDescriptor() );
    }

    interface Supplier
    {
        NewIndexDescriptor getNewIndexDescriptor();
    }
}
