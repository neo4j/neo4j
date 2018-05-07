/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.Optional;

import org.neo4j.graphdb.Label;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.neo4j.internal.kernel.api.schema.SchemaUtil.idTokenNameLookup;

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class IndexRule extends SchemaIndexDescriptor implements SchemaRule
{
    private final long id;
    private final Long owningConstraintId;

    public static IndexRule indexRule( long id, SchemaIndexDescriptor descriptor,
                                       IndexProvider.Descriptor providerDescriptor )
    {
        return new IndexRule( id, providerDescriptor, descriptor, null );
    }

    public static IndexRule constraintIndexRule( long id, SchemaIndexDescriptor descriptor,
                                                 IndexProvider.Descriptor providerDescriptor,
                                                 Long owningConstraint )
    {
        return new IndexRule( id, providerDescriptor, descriptor, owningConstraint );
    }

    public static IndexRule indexRule( long id, SchemaIndexDescriptor descriptor,
                                       IndexProvider.Descriptor providerDescriptor, String name )
    {
        return new IndexRule( id, providerDescriptor, descriptor, null, name );
    }

    public static IndexRule constraintIndexRule( long id, SchemaIndexDescriptor descriptor,
                                                 IndexProvider.Descriptor providerDescriptor,
                                                 Long owningConstraint, String name )
    {
        return new IndexRule( id, providerDescriptor, descriptor, owningConstraint, name );
    }

    IndexRule( long id, IndexProvider.Descriptor providerDescriptor,
               SchemaIndexDescriptor descriptor, Long owningConstraintId )
    {
        this( id, providerDescriptor, descriptor, owningConstraintId, null );
    }

    IndexRule( long id, IndexProvider.Descriptor providerDescriptor,
               SchemaIndexDescriptor descriptor, Long owningConstraintId, String name )
    {
        super( descriptor.schema(),
               descriptor.type(),
               Optional.of( descriptor.name().orElse( "index_" + id ) ),
               descriptor.providerDescriptor() );

        this.id = id;

        if ( descriptor.providerDescriptor() == null )
        {
            throw new IllegalArgumentException( "null provider descriptor prohibited" );
        }

        this.owningConstraintId = owningConstraintId;
    }

    public boolean canSupportUniqueConstraint()
    {
        return type() == SchemaIndexDescriptor.Type.UNIQUE;
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
        return owningConstraintId;
    }

    public boolean isIndexWithoutOwningConstraint()
    {
        return canSupportUniqueConstraint() && getOwningConstraint() == null;
    }

    public IndexRule withOwningConstraint( long constraintId )
    {
        if ( !canSupportUniqueConstraint() )
        {
            throw new IllegalStateException( this + " is not a constraint index" );
        }
        return constraintIndexRule( id, this, this.providerDescriptor(), constraintId );
    }

    @Override
    public String toString()
    {
        String ownerString = "";
        if ( canSupportUniqueConstraint() )
        {
            ownerString = ", owner=" + owningConstraintId;
        }

        return "IndexRule[id=" + id + ", descriptor=" + this.userDescription( idTokenNameLookup ) +
               ", provider=" + this.providerDescriptor() + ownerString + "]";
    }

    @Override
    public long getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return name().get();
    }
}
