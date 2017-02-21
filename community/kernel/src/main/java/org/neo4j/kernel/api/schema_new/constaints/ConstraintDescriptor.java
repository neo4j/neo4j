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
package org.neo4j.kernel.api.schema_new.constaints;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaComputer;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;

import static java.lang.String.format;
import static org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor.Type.UNIQUE;

/**
 * Internal representation of a graph constraint, including the schema unit it targets (eg. label-property combination)
 * and the how that schema unit is constrained (eg. "has to exist", or "must be unique").
 */
public class ConstraintDescriptor implements SchemaDescriptor.Supplier
{
    public enum Type { UNIQUE, EXISTS }

    public interface Supplier
    {
        ConstraintDescriptor getConstraintDescriptor();
    }

    private final SchemaDescriptor schema;
    private final ConstraintDescriptor.Type type;

    ConstraintDescriptor( SchemaDescriptor schema, Type type )
    {
        this.schema = schema;
        this.type = type;
    }

    // METHODS

    @Override
    public SchemaDescriptor schema()
    {
        return schema;
    }

    public NewIndexDescriptor ownedIndexDescriptor()
    {
        if ( type == UNIQUE && schema instanceof LabelSchemaDescriptor )
        {
            return NewIndexDescriptorFactory.uniqueForSchema( (LabelSchemaDescriptor)schema );
        }
        throw new IllegalStateException( "Only unique constraints on label-property combinations are allowed" );
    }

    public Type type()
    {
        return type;
    }

    /**
     * @param tokenNameLookup used for looking up names for token ids.
     * @return a user friendly description of this constraint.
     */
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( "Constraint( %s, %s )", type.name(), schema.userDescription( tokenNameLookup ) );
    }

    /**
     * Checks whether a constraint descriptor Supplier supplies this constraint descriptor.
     * @param supplier supplier to get a constraint descriptor from
     * @return true if the supplied constraint descriptor equals this constraint descriptor
     */
    public boolean isSame( Supplier supplier )
    {
        return this.equals( supplier.getConstraintDescriptor() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o != null && o instanceof ConstraintDescriptor )
        {
            ConstraintDescriptor that = (ConstraintDescriptor)o;
            return this.type() == that.type() && this.schema().equals( that.schema() );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return type.hashCode() & schema.hashCode();
    }

    // PRETTY PRINTING

    public String prettyPrint( TokenNameLookup tokenNameLookup )
    {
        return schema.computeWith( new ConstraintPrettyPrinter( tokenNameLookup ) );
    }

    private class ConstraintPrettyPrinter implements SchemaComputer<String>
    {
        private final TokenNameLookup tokenNameLookup;

        ConstraintPrettyPrinter( TokenNameLookup tokenNameLookup )
        {
            this.tokenNameLookup = tokenNameLookup;
        }

        @Override
        public String computeSpecific( LabelSchemaDescriptor schema )
        {
            assert schema.getPropertyIds().length == 1;
            String labelName = labelName( schema.getLabelId(), tokenNameLookup );
            String nodeName = labelName.toLowerCase();
            String propertyName = tokenNameLookup.propertyKeyGetName( schema.getPropertyIds()[0] );
            if ( type == UNIQUE )
            {
                return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT %s.%s IS UNIQUE",
                        nodeName, labelName, nodeName, propertyName );
            }
            else
            {
                return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT exists(%s.%s)",
                        nodeName, labelName, nodeName, propertyName );
            }
        }

        @Override
        public String computeSpecific( RelationTypeSchemaDescriptor schema )
        {
            assert schema.getPropertyIds().length == 1;
            String typeName = tokenNameLookup.relationshipTypeGetName( schema.getRelTypeId() );
            String relName = typeName.toLowerCase();
            String propertyName = tokenNameLookup.propertyKeyGetName( schema.getPropertyIds()[0] );
            return String.format( "CONSTRAINT ON ()-[ %s:%s ]-() ASSERT exists(%s.%s)",
                    relName, typeName, relName, propertyName );
        }

        private String labelName( int labelId, TokenNameLookup tokenNameLookup )
        {
            String labelName = tokenNameLookup.labelGetName( labelId );
            //if the labelName contains a `:` we must escape it to avoid disambiguation,
            //e.g. CONSTRAINT on foo:bar:foo:bar
            if (labelName.contains( ":" )) {
                return "`" + labelName + "`";
            }
            else
            {
                return labelName;
            }
        }
    }
}
