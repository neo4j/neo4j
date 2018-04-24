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

import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.neo4j.internal.kernel.api.schema.SchemaUtil.idTokenNameLookup;

/**
 * A {@link Label} can have zero or more index rules which will have data specified in the rules indexed.
 */
public class IndexRule extends SchemaRule implements IndexDescriptor.Supplier
{
    private final IndexProvider.Descriptor providerDescriptor;
    private final SchemaDescriptor descriptor;
    private final Long owningConstraint;
    private final String metadata;
    private final IndexDescriptor.Type type;

    //TODO clean up this factory here...
    public static IndexRule indexRule( long id, SchemaDescriptor descriptor, IndexProvider.Descriptor providerDescriptor, IndexDescriptor.Type type )
    {
        return new IndexRule( id, providerDescriptor, descriptor, null, type );
    }

    public static IndexRule constraintIndexRule( long id, IndexDescriptor descriptor,
                                                 IndexProvider.Descriptor providerDescriptor,
                                                 Long owningConstraint )
    {
        return new IndexRule( id, providerDescriptor, descriptor.schema(), owningConstraint, descriptor.type() );
    }

    public static IndexRule indexRule( long id, SchemaDescriptor descriptor, IndexProvider.Descriptor providerDescriptor, String name, String metadata,
            IndexDescriptor.Type type )
    {
        return new IndexRule( id, providerDescriptor, descriptor, null, name, metadata, type );
    }

    public static IndexRule constraintIndexRule( long id, SchemaIndexDescriptor descriptor,
                                                 IndexProvider.Descriptor providerDescriptor,
                                                 Long owningConstraint, String name )
    {
        return new IndexRule( id, providerDescriptor, descriptor.schema(), owningConstraint, name, "", descriptor.type() );
    }

    private IndexRule constraintIndexRule( long id, SchemaDescriptor descriptor, IndexProvider.Descriptor providerDescriptor, long owningConstraint, IndexDescriptor.Type type )
    {
        return new IndexRule( id, providerDescriptor, descriptor, owningConstraint, null, "", type );
    }

    IndexRule( long id, IndexProvider.Descriptor providerDescriptor,
            SchemaDescriptor descriptor, Long owningConstraint, IndexDescriptor.Type type )
    {
        this( id, providerDescriptor, descriptor, owningConstraint, null, "", type );
    }

    IndexRule( long id, IndexProvider.Descriptor providerDescriptor, SchemaDescriptor descriptor, Long owningConstraint, String name, String metadata,
            IndexDescriptor.Type type )
    {
        super( id, name );
        if ( providerDescriptor == null )
        {
            throw new IllegalArgumentException( "null provider descriptor prohibited" );
        }

        this.descriptor = descriptor;
        this.owningConstraint = owningConstraint;
        this.providerDescriptor = providerDescriptor;
        this.metadata = metadata;
        this.type = type;
    }

    public IndexProvider.Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    public boolean canSupportUniqueConstraint()
    {
        return type == IndexDescriptor.Type.UNIQUE;
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
        //TODO should the name live on?
        return constraintIndexRule( id, descriptor, providerDescriptor, constraintId, type );
    }

    @Override
    public byte[] serialize()
    {
        return SchemaRuleSerialization.serialize( this );
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
    public SchemaDescriptor schema()
    {
        return descriptor.schema();
    }

    @Override
    public IndexDescriptor getIndexDescriptor( IndexProviderMap indexProviderMap )
    {
        return indexProviderMap.get( providerDescriptor ).indexDescriptorFor( descriptor, name, metadata );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o instanceof IndexRule )
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

    public IndexDescriptor.Type type()
    {
        return type;
    }

    public String getMetadata()
    {
        return metadata;
    }
}
