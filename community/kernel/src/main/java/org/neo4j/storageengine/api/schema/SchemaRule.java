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
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;

/**
 * Represents a stored schema rule.
 */
public abstract class SchemaRule implements SchemaDescriptor.Supplier
{
    protected final long id;
    protected String name;

    protected SchemaRule( long id )
    {
        this.id = id;
    }

    /**
     * The persistence id for this rule.
     */
    public final long getId()
    {
        return this.id;
    }

    /**
     * @return The (possibly user supplied) name of this schema rule.
     */
    public final String getName()
    {
        if ( name == null )
        {
            name = SchemaRule.generateName( this );
        }
        return name;
    }

    public abstract byte[] serialize();

    /**
     * Set the name of this rule, if it has not been set already.
     *
     * Note that once a name has been observed - either via this method or via {@link #getName()} – for this rule,
     * it cannot be changed.
     * @param name The name desired for this rule.
     * @return {@code true} if the name was set, {@code false} if this rule already has a name.
     */
    public final boolean setName( String name )
    {
        if ( this.name == null )
        {
            if ( name == null )
            {
                return true;
            }
            if ( name.length() == 0 )
            {
                throw new IllegalArgumentException( "Rule name cannot be the empty string" );
            }
            for ( int i = 0; i < name.length(); i++ )
            {
                char ch = name.charAt( i );
                if ( ch == 0 )
                {
                    throw new IllegalArgumentException( "Illegal rule name: '" + name + "'" );
                }
            }
            this.name = name;
            return true;
        }
        return false;
    }

    public static String generateName( SchemaRule rule )
    {
        Class<? extends SchemaRule> type = rule.getClass();
        long id = rule.getId();
        if ( type == IndexRule.class )
        {
            return "index_" + id;
        }
        if ( type == ConstraintRule.class )
        {
            return "constraint_" + id;
        }
        return "schema_" + id;
    }

    /**
     * This enum is used for the legacy schema store, and should not be extended.
     */
    @Deprecated
    public enum Kind
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
                return descriptor.schema().computeWith( existenceKindMapper );
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
