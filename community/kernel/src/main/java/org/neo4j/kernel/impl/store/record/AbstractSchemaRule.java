/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.storageengine.api.schema.SchemaRule;

public abstract class AbstractSchemaRule implements SchemaRule, RecordSerializable
{
    protected final Kind kind;
    protected final long id;

    public AbstractSchemaRule( long id, Kind kind )
    {
        this.id = id;
        this.kind = kind;
    }

    @Override
    public long getId()
    {
        return this.id;
    }

    @Override
    public final Kind getKind()
    {
        return this.kind;
    }

    @Override
    public abstract int length();

    @Override
    public abstract void serialize( ByteBuffer target );

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
        return kind == that.kind;
    }

    @Override
    public int hashCode()
    {
        return kind.hashCode();
    }

    @Override
    public abstract String toString();

    public static SchemaRule deserialize( long id, ByteBuffer buffer ) throws MalformedSchemaRuleException
    {
        int labelId = buffer.getInt();
        Kind kind = Kind.forId( buffer.get() );
        try
        {
            SchemaRule rule = newRule( kind, id, labelId, buffer );
            if ( null == rule )
            {
                throw new MalformedSchemaRuleException( null,
                        "Deserialized null schema rule for id %d with kind %s", id, kind.name() );
            }
            return rule;
        }
        catch ( Exception e )
        {
            throw new MalformedSchemaRuleException( e,
                    "Could not deserialize schema rule for id %d with kind %s", id, kind.name() );
        }
    }

    protected static SchemaRule newRule( Kind kind, long id, int labelId, ByteBuffer buffer )
    {
        switch ( kind )
        {
        case INDEX_RULE:
            return IndexRule.readIndexRule( id, false, labelId, buffer );
        case CONSTRAINT_INDEX_RULE:
            return IndexRule.readIndexRule( id, true, labelId, buffer );
        case UNIQUENESS_CONSTRAINT:
            return UniquePropertyConstraintRule.readUniquenessConstraintRule( id, labelId, buffer );
        case NODE_PROPERTY_EXISTENCE_CONSTRAINT:
            return NodePropertyExistenceConstraintRule.readNodePropertyExistenceConstraintRule( id, labelId, buffer );
        case RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT:
            return RelationshipPropertyExistenceConstraintRule
                    .readRelPropertyExistenceConstraintRule( id, labelId, buffer );
        default:
            throw new IllegalArgumentException( kind.name() );
        }
    }

    public static Class<?> getRuleClass( Kind kind )
    {
        switch ( kind )
        {
        case INDEX_RULE:
        case CONSTRAINT_INDEX_RULE:
            return IndexRule.class;
        case UNIQUENESS_CONSTRAINT:
            return UniquePropertyConstraintRule.class;
        case NODE_PROPERTY_EXISTENCE_CONSTRAINT:
            return NodePropertyExistenceConstraintRule.class;
        case RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT:
            return RelationshipPropertyExistenceConstraintRule.class;
        default:
            throw new IllegalArgumentException( kind.name() );
        }
    }
}
