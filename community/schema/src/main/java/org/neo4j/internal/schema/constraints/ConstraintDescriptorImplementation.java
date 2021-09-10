/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.schema.constraints;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaNameUtil;
import org.neo4j.internal.schema.SchemaUserDescription;
import org.neo4j.util.Preconditions;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.internal.schema.ConstraintType.EXISTS;

/**
 * Internal representation of a graph constraint, including the schema unit it targets (eg. label-property combination)
 * and how that schema unit is constrained (eg. "has to exist", or "must be unique").
 */
public class ConstraintDescriptorImplementation implements ConstraintDescriptor, NodeExistenceConstraintDescriptor, NodeKeyConstraintDescriptor,
        RelExistenceConstraintDescriptor, UniquenessConstraintDescriptor
{
    private final ConstraintType type;
    private final SchemaDescriptor schema;
    private final long id;
    private final String name;
    private final Long ownedIndex;
    private final IndexType ownedIndexType;

    ConstraintDescriptorImplementation( ConstraintType type, SchemaDescriptor schema )
    {
        Preconditions.checkState( type == EXISTS, "Index type should be supplied for index-backed constraints" );
        this.type = type;
        this.schema = schema;
        this.id = NO_ID;
        this.name = null;
        this.ownedIndex = null;
        this.ownedIndexType = null;
    }

    ConstraintDescriptorImplementation( ConstraintType type, SchemaDescriptor schema, IndexType ownedIndexType )
    {
        this.type = type;
        this.schema = schema;
        this.id = NO_ID;
        this.name = null;
        this.ownedIndex = null;
        Preconditions.checkState( (type != EXISTS && ownedIndexType != null) || (type == EXISTS && ownedIndexType == null),
                "Index type should be supplied for index-backed constraints" );
        this.ownedIndexType = ownedIndexType;
    }

    private ConstraintDescriptorImplementation( ConstraintType type, SchemaDescriptor schema, long id, String name, Long ownedIndex, IndexType ownedIndexType )
    {
        this.type = type;
        this.schema = schema;
        this.id = id;
        this.name = name;
        this.ownedIndex = ownedIndex;
        this.ownedIndexType = ownedIndexType;
    }

    // METHODS

    @Override
    public ConstraintType type()
    {
        return type;
    }

    @Override
    public SchemaDescriptor schema()
    {
        return schema;
    }

    @Override
    public boolean enforcesUniqueness()
    {
        return type.enforcesUniqueness();
    }

    @Override
    public boolean enforcesPropertyExistence()
    {
        return type.enforcesPropertyExistence();
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user-friendly description of this constraint.
     */
    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return SchemaUserDescription.forConstraint( tokenNameLookup, id, name, type, schema(), ownedIndex );
    }

    @Override
    public boolean isRelationshipPropertyExistenceConstraint()
    {
        return schema.entityType() == EntityType.RELATIONSHIP && type == EXISTS;
    }

    @Override
    public RelExistenceConstraintDescriptor asRelationshipPropertyExistenceConstraint()
    {
        if ( !isRelationshipPropertyExistenceConstraint() )
        {
            throw conversionException( RelExistenceConstraintDescriptor.class );
        }
        return this;
    }

    @Override
    public boolean isNodePropertyExistenceConstraint()
    {
        return schema.entityType() == NODE && type == EXISTS;
    }

    @Override
    public NodeExistenceConstraintDescriptor asNodePropertyExistenceConstraint()
    {
        if ( !isNodePropertyExistenceConstraint() )
        {
            throw conversionException( NodeExistenceConstraintDescriptor.class );
        }
        return this;
    }

    @Override
    public boolean isUniquenessConstraint()
    {
        return schema.entityType() == NODE && type == ConstraintType.UNIQUE;
    }

    @Override
    public UniquenessConstraintDescriptor asUniquenessConstraint()
    {
        if ( !isUniquenessConstraint() )
        {
            throw conversionException( UniquenessConstraintDescriptor.class );
        }
        return this;
    }

    @Override
    public boolean isNodeKeyConstraint()
    {
        return schema.entityType() == NODE && type == ConstraintType.UNIQUE_EXISTS;
    }

    @Override
    public NodeKeyConstraintDescriptor asNodeKeyConstraint()
    {
        if ( !isNodeKeyConstraint() )
        {
            throw conversionException( NodeKeyConstraintDescriptor.class );
        }
        return this;
    }

    @Override
    public boolean isIndexBackedConstraint()
    {
        return isUniquenessConstraint() || isNodeKeyConstraint();
    }

    @Override
    public IndexBackedConstraintDescriptor asIndexBackedConstraint()
    {
        if ( !isIndexBackedConstraint() )
        {
            throw conversionException( IndexBackedConstraintDescriptor.class );
        }
        return this;
    }

    private IllegalStateException conversionException( Class<? extends ConstraintDescriptor> targetType )
    {
        return new IllegalStateException( "Cannot cast this schema to a " + targetType + " because it does not match that structure: " + this + "." );
    }

    @Override
    public final boolean equals( Object o )
    {
        if ( o instanceof ConstraintDescriptor )
        {
            ConstraintDescriptor that = (ConstraintDescriptor) o;
            boolean compare = this.type() == that.type() && this.schema().equals( that.schema() );
            if ( compare && that.isIndexBackedConstraint() )
            {
                compare = compare && this.indexType().equals( that.asIndexBackedConstraint().indexType() );
            }
            return compare;
        }
        return false;
    }

    @Override
    public final int hashCode()
    {
        return type.hashCode() & schema().hashCode();
    }

    @Override
    public long getId()
    {
        if ( id == NO_ID )
        {
            throw new IllegalStateException( "This constraint descriptor have no id assigned: " + this );
        }
        return id;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public boolean hasOwnedIndexId()
    {
        return ownedIndex != null;
    }

    @Override
    public long ownedIndexId()
    {
        if ( ownedIndex == null )
        {
            throw new IllegalStateException( "This constraint does not own an index." );
        }
        return ownedIndex;
    }

    @Override
    public IndexType indexType()
    {
        if ( ownedIndexType == null )
        {
            throw new IllegalStateException( "This constraint does not own an index." );
        }
        return ownedIndexType;
    }

    @Override
    public ConstraintDescriptorImplementation withId( long id )
    {
        return new ConstraintDescriptorImplementation( type, schema, id, name, ownedIndex, ownedIndexType );
    }

    @Override
    public ConstraintDescriptorImplementation withName( String name )
    {
        if ( name == null )
        {
            return this;
        }
        name = SchemaNameUtil.sanitiseName( name );
        return new ConstraintDescriptorImplementation( type, schema, id, name, ownedIndex, ownedIndexType );
    }

    @Override
    public ConstraintDescriptorImplementation withOwnedIndexId( long ownedIndex )
    {
        Preconditions.checkState( ownedIndexType != null, "ConstraintDescriptor missing IndexType when connected to index" );
        return new ConstraintDescriptorImplementation( type, schema, id, name, ownedIndex, ownedIndexType );
    }
}
