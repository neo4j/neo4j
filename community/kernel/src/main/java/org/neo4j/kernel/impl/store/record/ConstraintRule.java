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
package org.neo4j.kernel.impl.store.record;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.neo4j.internal.kernel.api.schema.SchemaUtil.idTokenNameLookup;

public class ConstraintRule implements SchemaRule, ConstraintDescriptor.Supplier
{
    private final Long ownedIndex;
    private final String name;
    private final long id;
    private final ConstraintDescriptor descriptor;

    public static ConstraintRule constraintRule(
            long id, ConstraintDescriptor descriptor )
    {
        return new ConstraintRule( id, descriptor, null );
    }

    public static ConstraintRule constraintRule(
            long id, IndexBackedConstraintDescriptor descriptor, long ownedIndexRule )
    {
        return new ConstraintRule( id, descriptor, ownedIndexRule );
    }

    public static ConstraintRule constraintRule(
            long id, ConstraintDescriptor descriptor, String name )
    {
        return new ConstraintRule( id, descriptor, null, name );
    }

    public static ConstraintRule constraintRule(
            long id, IndexBackedConstraintDescriptor descriptor, long ownedIndexRule, String name )
    {
        return new ConstraintRule( id, descriptor, ownedIndexRule, name );
    }

    ConstraintRule( long id, ConstraintDescriptor descriptor, Long ownedIndex )
    {
        this( id, descriptor, ownedIndex, null );
    }

    ConstraintRule( long id, ConstraintDescriptor descriptor, Long ownedIndex, String name )
    {
        this.id = id;
        this.descriptor = descriptor;
        this.ownedIndex = ownedIndex;
        this.name = SchemaRule.nameOrDefault( name, "constraint_" + id );
    }

    @Override
    public String toString()
    {
        return "ConstraintRule[id=" + id + ", descriptor=" + descriptor.userDescription( idTokenNameLookup ) + ", " +
                "ownedIndex=" + ownedIndex + "]";
    }

    @Override
    public SchemaDescriptor schema()
    {
        return descriptor.schema();
    }

    @Override
    public ConstraintDescriptor getConstraintDescriptor()
    {
        return descriptor;
    }

    @SuppressWarnings( "NumberEquality" )
    public long getOwnedIndex()
    {
        if ( ownedIndex == null )
        {
            throw new IllegalStateException( "This constraint does not own an index." );
        }
        return ownedIndex;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o instanceof ConstraintRule )
        {
            ConstraintRule that = (ConstraintRule) o;
            return this.descriptor.equals( that.descriptor );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return descriptor.hashCode();
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return name;
    }
}
