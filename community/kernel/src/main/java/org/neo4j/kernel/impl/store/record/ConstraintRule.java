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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;

import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.neo4j.kernel.api.schema_new.SchemaUtil.idTokenNameLookup;

public class ConstraintRule implements SchemaRule, ConstraintDescriptor.Supplier
{
    private final long id;
    private final Long ownedIndexRule;
    private final ConstraintDescriptor descriptor;

    @Override
    public final long getId()
    {
        return this.id;
    }

    public static ConstraintRule constraintRule(
            long id, ConstraintDescriptor descriptor )
    {
        return new ConstraintRule( id, descriptor, null );
    }

    public static ConstraintRule constraintRule(
            long id, ConstraintDescriptor descriptor, long ownedIndexRule )
    {
        return new ConstraintRule( id, descriptor, ownedIndexRule );
    }

    ConstraintRule( long id, ConstraintDescriptor descriptor, Long ownedIndexRule )
    {
        this.id = id;
        this.descriptor = descriptor;
        this.ownedIndexRule = ownedIndexRule;
    }

    @Override
    public String toString()
    {
        return "ConstraintRule[id=" + id + ", descriptor=" + descriptor.userDescription( idTokenNameLookup ) + ", " +
                "ownedIndex=" + ownedIndexRule + "]";
    }

    @Override
    public SchemaDescriptor schema()
    {
        return descriptor.schema();
    }

    public ConstraintDescriptor getConstraintDescriptor()
    {
        return descriptor;
    }

    @SuppressWarnings( "NumberEquality" )
    public long getOwnedIndex()
    {
        if ( ownedIndexRule == null )
        {
            throw new IllegalStateException( "This constraint does not own an index." );
        }
        return ownedIndexRule;
    }

    @Override
    public int length()
    {
        return SchemaRuleSerialization.lengthOf( this );
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        SchemaRuleSerialization.serialize( this, target );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o != null && o instanceof ConstraintRule )
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
}
