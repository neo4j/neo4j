/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

public abstract class AbstractSchemaRule implements SchemaRule
{
    /**
     * Idea to keep both label and relationship type ids in this class is definitely incorrect.
     * This is a temporary measure to minimize amount of changes in an unrelated feature commit.
     * Such design should be fixed when we fuse hierarchies of
     * {@link org.neo4j.kernel.api.constraints.PropertyConstraint} and {@link SchemaRule}. This could be done because
     * both are internal representations of indexes/constraints and it is wasteful and confusing to have two internal
     * representations in addition to one external {@link org.neo4j.graphdb.schema.ConstraintDefinition}
     */

    protected static final int NOT_INITIALIZED = -1;

    private final int label;
    private final int relationshipType;
    private final Kind kind;
    private final long id;

    public AbstractSchemaRule( long id, int label, int relationshipType, Kind kind )
    {
        verifyLabelAndRelType( label, relationshipType );
        this.id = id;
        this.label = label;
        this.relationshipType = relationshipType;
        this.kind = kind;
    }

    @Override
    public long getId()
    {
        return this.id;
    }

    protected final int getLabelOrRelationshipType()
    {
        return (label == NOT_INITIALIZED) ? relationshipType : label;
    }

    @Override
    public final Kind getKind()
    {
        return this.kind;
    }

    @Override
    public int length()
    {
        return 4 /*label or relationshipType id*/ + 1 /*kind id*/;
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        target.putInt( getLabelOrRelationshipType() );
        target.put( kind.id() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        AbstractSchemaRule that = (AbstractSchemaRule) o;
        return label == that.label && relationshipType == that.relationshipType && kind == that.kind;
    }

    @Override
    public int hashCode()
    {
        return 31 * (31 * label + relationshipType) + kind.hashCode();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[id=" + id + ", label=" + label +
               " relationshipType=" + relationshipType + ", kind=" + kind + innerToString() + "]";
    }

    protected abstract String innerToString();

    private static void verifyLabelAndRelType( int label, int relationshipType )
    {
        if ( label == NOT_INITIALIZED && relationshipType == NOT_INITIALIZED )
        {
            throw new IllegalArgumentException( "Either label or relationshipType should be initialized" );
        }
        if ( label != NOT_INITIALIZED && relationshipType != NOT_INITIALIZED )
        {
            throw new IllegalArgumentException( "Both label and relationshipType can't be initialized" );
        }
    }
}
