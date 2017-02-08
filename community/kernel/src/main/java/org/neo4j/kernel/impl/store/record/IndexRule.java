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

import org.neo4j.graphdb.Label;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.neo4j.kernel.api.schema_new.SchemaUtil.idTokenNameLookup;

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class IndexRule implements SchemaRule, NewIndexDescriptor.Supplier
{
    private final long id;
    private final SchemaIndexProvider.Descriptor providerDescriptor;
    private final NewIndexDescriptor descriptor;
    private final Long owningConstraint;

    public static IndexRule indexRule( long id, NewIndexDescriptor descriptor,
                                       SchemaIndexProvider.Descriptor providerDescriptor )
    {
        return new IndexRule( id, providerDescriptor, descriptor, null );
    }

    public static IndexRule constraintIndexRule( long id, NewIndexDescriptor descriptor,
                                                 SchemaIndexProvider.Descriptor providerDescriptor,
                                                 Long owningConstraint )
    {
        return new IndexRule( id, providerDescriptor, descriptor, owningConstraint );
    }

    IndexRule( long id, SchemaIndexProvider.Descriptor providerDescriptor,
            NewIndexDescriptor descriptor, Long owningConstraint )
    {
        this.id = id;
        if ( providerDescriptor == null )
        {
            throw new IllegalArgumentException( "null provider descriptor prohibited" );
        }

        this.descriptor = descriptor;
        this.owningConstraint = owningConstraint;
        this.providerDescriptor = providerDescriptor;
    }

    public SchemaIndexProvider.Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    public boolean canSupportUniqueConstraint()
    {
        return descriptor.type() == NewIndexDescriptor.Type.UNIQUE;
    }

    /**
     * Return the owning constraints of this index.
     *
     * The owning constraint can be null during the construction of a uniqueness constraint. This construction first
     * creates the unique index, and then waits for the index to become fully populated and online before creating
     * the actual constraint. During unique index population the owning constraint will be null.
     *
     * See ConstraintIndexCreator.createUniquenessConstraintIndex().
     *
     * @return the id of the owning constraint, or null if this has not been set yet.
     * @throws IllegalStateException if this IndexRule cannot support uniqueness constraints (ei. the index is not
     *                               unique)
     */
    public Long getOwningConstraint()
    {
        if ( !canSupportUniqueConstraint() )
        {
            throw new IllegalStateException( "Can only get owner from constraint indexes." );
        }
        return owningConstraint;
    }

    public IndexRule withOwningConstraint( long constraintId )
    {
        if ( !canSupportUniqueConstraint() )
        {
            throw new IllegalStateException( this + " is not a constraint index" );
        }
        return constraintIndexRule( id, descriptor, providerDescriptor, constraintId );
    }

    @Override
    public long getId()
    {
        return id;
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
    public String toString()
    {
        String ownerString = "";
        if ( canSupportUniqueConstraint() )
        {
            ownerString = ", owner=" + owningConstraint;
        }

        return "IndexRule[id=" + id + ", descriptor=" + descriptor.userDescription( idTokenNameLookup ) +
               ", provider=" + providerDescriptor + ownerString + "]";
    }

    @Override
    public LabelSchemaDescriptor schema()
    {
        return descriptor.schema();
    }

    public NewIndexDescriptor getIndexDescriptor()
    {
        return descriptor;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o != null && o instanceof IndexRule )
        {
            IndexRule that = (IndexRule) o;
            return this.descriptor.equals( that.descriptor );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.descriptor.hashCode();
    }
}
