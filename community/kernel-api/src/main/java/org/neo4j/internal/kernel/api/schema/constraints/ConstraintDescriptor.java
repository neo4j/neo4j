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
package org.neo4j.internal.kernel.api.schema.constraints;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;

public interface ConstraintDescriptor extends SchemaDescriptorSupplier
{
    enum Type
    {
        UNIQUE( true, false ),
        EXISTS( false, true ),
        UNIQUE_EXISTS( true, true );

        private final boolean isUnique;
        private final boolean mustExist;

        Type( boolean isUnique, boolean mustExist )
        {
            this.isUnique = isUnique;
            this.mustExist = mustExist;
        }

        public boolean enforcesUniqueness()
        {
            return isUnique;
        }

        public boolean enforcesPropertyExistence()
        {
            return mustExist;
        }
    }

    @Override
    SchemaDescriptor schema();

    Type type();

    boolean enforcesUniqueness();

    boolean enforcesPropertyExistence();

    String userDescription( TokenNameLookup tokenNameLookup );

    /**
     * Checks whether a constraint descriptor Supplier supplies this constraint descriptor.
     * @param supplier supplier to get a constraint descriptor from
     * @return true if the supplied constraint descriptor equals this constraint descriptor
     */
    boolean isSame( Supplier supplier );

    interface Supplier
    {
        ConstraintDescriptor getConstraintDescriptor();
    }

    String prettyPrint( TokenNameLookup tokenNameLookup );
}
