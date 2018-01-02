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

import java.nio.ByteBuffer;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;

public interface SchemaRule extends RecordSerializable
{
    /**
     * The persistence id for this rule.
     */
    long getId();

    /**
     * @return id of label to which this schema rule has been attached
     * @throws IllegalStateException when this rule has kind {@link Kind#RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT}
     */
    int getLabel();

    /**
     * @return id of relationship type to which this schema rule has been attached
     * @throws IllegalStateException when this rule has kind different from
     * {@link Kind#RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT}
     */
    int getRelationshipType();

    /**
     * @return the kind of this schema rule
     */
    Kind getKind();

    enum Kind
    {
        INDEX_RULE( 1, IndexRule.class )
        {
            @Override
            protected SchemaRule newRule( long id, int labelId, ByteBuffer buffer )
            {
                return IndexRule.readIndexRule( id, false, labelId, buffer );
            }
        },
        CONSTRAINT_INDEX_RULE( 2, IndexRule.class )
        {
            @Override
            protected SchemaRule newRule( long id, int labelId, ByteBuffer buffer )
            {
                return IndexRule.readIndexRule( id, true, labelId, buffer );
            }
        },
        UNIQUENESS_CONSTRAINT( 3, UniquePropertyConstraintRule.class )
        {
            @Override
            protected SchemaRule newRule( long id, int labelId, ByteBuffer buffer )
            {
                return UniquePropertyConstraintRule.readUniquenessConstraintRule( id, labelId, buffer );
            }
        },
        NODE_PROPERTY_EXISTENCE_CONSTRAINT( 4, NodePropertyExistenceConstraintRule.class )
        {
            @Override
            protected SchemaRule newRule( long id, int labelId, ByteBuffer buffer )
            {
                return NodePropertyExistenceConstraintRule
                        .readNodePropertyExistenceConstraintRule( id, labelId, buffer );
            }
        },
        RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT( 5, RelationshipPropertyExistenceConstraintRule.class )
        {
            @Override
            protected SchemaRule newRule( long id, int labelId, ByteBuffer buffer )
            {
                return RelationshipPropertyExistenceConstraintRule
                        .readRelPropertyExistenceConstraintRule( id, labelId, buffer );
            }
        };

        private final byte id;
        private final Class<? extends SchemaRule> ruleClass;

        private Kind( int id, Class<? extends SchemaRule> ruleClass )
        {
            assert id > 0 : "Kind id 0 is reserved";
            this.id = (byte) id;
            this.ruleClass = ruleClass;
        }

        public Class<? extends SchemaRule> getRuleClass()
        {
            return this.ruleClass;
        }

        public byte id()
        {
            return this.id;
        }

        protected abstract SchemaRule newRule( long id, int labelId, ByteBuffer buffer );

        public static SchemaRule deserialize( long id, ByteBuffer buffer ) throws MalformedSchemaRuleException
        {
            int labelId = buffer.getInt();
            Kind kind = kindForId( buffer.get() );
            try
            {
                SchemaRule rule = kind.newRule( id, labelId, buffer );
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

        public static Kind kindForId( byte id ) throws MalformedSchemaRuleException
        {
            switch ( id )
            {
            case 1: return INDEX_RULE;
            case 2: return CONSTRAINT_INDEX_RULE;
            case 3: return UNIQUENESS_CONSTRAINT;
            case 4: return NODE_PROPERTY_EXISTENCE_CONSTRAINT;
            case 5: return RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT;
            default:
                throw new MalformedSchemaRuleException( null, "Unknown kind id %d", id );
            }
        }

        public boolean isIndex()
        {
            return ruleClass == IndexRule.class;
        }
    }
}
