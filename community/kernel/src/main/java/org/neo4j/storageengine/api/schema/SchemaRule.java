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
package org.neo4j.storageengine.api.schema;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaComputer;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.impl.store.record.RecordSerializable;

/**
 * Represents a stored schema rule.
 */
public interface SchemaRule extends SchemaDescriptor.Supplier, RecordSerializable
{
    /**
     * The persistence id for this rule.
     */
    long getId();

    enum Kind
    {
        INDEX_RULE( "Index" ),
        CONSTRAINT_INDEX_RULE( "Constraint index" ),
        UNIQUENESS_CONSTRAINT( "Uniqueness constraint" ),
        NODE_PROPERTY_EXISTENCE_CONSTRAINT( "Node property existence constraint" ),
        RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT( "Relationship property existence constraint" );

        private static final Kind[] ALL = values();

        private final String userString;

        Kind( String userString )
        {
            this.userString = userString;
        }

        public byte id()
        {
            return (byte) (ordinal() + 1);
        }

        public String userString()
        {
            return userString;
        }

        public static Kind forId( byte id ) throws MalformedSchemaRuleException
        {
            if ( id >= 1 && id <= ALL.length )
            {
                return values()[id-1];
            }
            throw new MalformedSchemaRuleException( null, "Unknown kind id %d", id );
        }

        public static Kind map( NewIndexDescriptor descriptor )
        {
            switch ( descriptor.type() )
            {
            case GENERAL:
                return INDEX_RULE;
            case UNIQUE:
                return CONSTRAINT_INDEX_RULE;
            default:
                throw new IllegalStateException( "Cannot end up here, says johant" );
            }
        }

        public static Kind map( ConstraintDescriptor descriptor )
        {
            switch ( descriptor.type() )
            {
            case UNIQUE:
                return UNIQUENESS_CONSTRAINT;
            case EXISTS:
                return existenceKindMapper.compute( descriptor.schema() );
            default:
                throw new IllegalStateException( "Cannot end up here, says johant" );
            }
        }

        private static SchemaComputer<Kind> existenceKindMapper = new SchemaComputer<Kind>()
        {
            @Override
            public Kind computeSpecific( LabelSchemaDescriptor schema )
            {
                return NODE_PROPERTY_EXISTENCE_CONSTRAINT;
            }

            @Override
            public Kind computeSpecific( RelationTypeSchemaDescriptor schema )
            {
                return RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT;
            }
        };
    }
}
