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
package org.neo4j.storageengine.api.schema;

import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaComputer;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;

/**
 * Represents a stored schema rule.
 */
public interface SchemaRule extends SchemaDescriptorSupplier
{
    static String nameOrDefault( String name, String defaultName )
    {
        if ( name == null )
        {
            return defaultName;
        }
        else if ( name.isEmpty() )
        {
            throw new IllegalArgumentException( "Schema rule name cannot be the empty string" );
        }
        else
        {
            int length = name.length();
            for ( int i = 0; i < length; i++ )
            {
                char ch = name.charAt( i );
                if ( ch == '\0' )
                {
                    throw new IllegalArgumentException( "Illegal schema rule name: '" + name + "'" );
                }
            }
        }
        return name;
    }

    static String checkName( String name )
    {
        if ( name == null || name.isEmpty() )
        {
            throw new IllegalArgumentException( "Schema rule name cannot be the empty string" );
        }
        else
        {
            int length = name.length();
            for ( int i = 0; i < length; i++ )
            {
                char ch = name.charAt( i );
                if ( ch == '\0' )
                {
                    throw new IllegalArgumentException( "Illegal schema rule name: '" + name + "'" );
                }
            }
        }
        return name;
    }

    /**
     * The persistence id for this rule.
     */
    long getId();

    /**
     * @return The (possibly user supplied) name of this schema rule.
     */
    String getName();

    /**
     * This enum is used for the legacy schema store, and should not be extended.
     * @see org.neo4j.kernel.impl.store.record.SchemaRuleSerialization for the new (de)serialisation code instead.
     */
    @Deprecated
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
                return values()[id - 1];
            }
            throw new MalformedSchemaRuleException( null, "Unknown kind id %d", id );
        }

        public static Kind map( IndexDescriptor descriptor )
        {
            switch ( descriptor.type() )
            {
            case GENERAL:
                return INDEX_RULE;
            case UNIQUE:
                return CONSTRAINT_INDEX_RULE;
            default:
                throw new IllegalStateException(
                        "Cannot map descriptor type to legacy schema rule: " + descriptor.type() );
            }
        }

        public static Kind map( ConstraintDescriptor descriptor )
        {
            switch ( descriptor.type() )
            {
            case UNIQUE:
                return UNIQUENESS_CONSTRAINT;
            case EXISTS:
                return descriptor.schema().computeWith( existenceKindMapper );
            default:
                throw new IllegalStateException(
                        "Cannot map descriptor type to legacy schema rule: " + descriptor.type() );
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

            @Override
            public Kind computeSpecific( SchemaDescriptor schema )
            {
                throw new IllegalStateException( "General schema rules cannot support constraints" );
            }
        };
    }
}
