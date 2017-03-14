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
package org.neo4j.kernel.impl.util.dbstructure;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.helpers.Strings;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;

import static java.lang.String.format;

public enum DbStructureArgumentFormatter implements ArgumentFormatter
{
    INSTANCE;

    private static List<String> IMPORTS = Arrays.asList(
            UniquenessConstraint.class.getCanonicalName(),
            LabelSchemaDescriptor.class.getCanonicalName(),
            SchemaDescriptorFactory.class.getCanonicalName(),
            NewIndexDescriptor.class.getCanonicalName(),
            NewIndexDescriptorFactory.class.getCanonicalName()
    );

    @Override
    public Collection<String> imports()
    {
        return IMPORTS;
    }

    public void formatArgument( Appendable builder, Object arg ) throws IOException
    {
        if ( arg == null )
        {
            builder.append( "null" );
        }
        else if ( arg instanceof String )
        {
            builder.append( '"' );
            Strings.escape( builder, arg.toString() );
            builder.append( '"' );
        }
        else if ( arg instanceof Long )
        {
            builder.append( arg.toString() );
            builder.append( 'L' );
        }
        else if ( arg instanceof Integer )
        {
            builder.append( arg.toString() );
        }
        else if ( arg instanceof Double )
        {
            double d = (Double) arg;
            if ( Double.isNaN( d ) )
            {
                builder.append( "Double.NaN" );
            }
            else if ( Double.isInfinite( d ) )
            {
                builder.append( d < 0 ? "Double.NEGATIVE_INFINITY" : "Double.POSITIVE_INFINITY" );
            }
            else
            {
                builder.append( arg.toString() );
                builder.append( 'd' );
            }
        }
        else if ( arg instanceof NewIndexDescriptor )
        {
            NewIndexDescriptor descriptor = (NewIndexDescriptor) arg;
            int labelId = descriptor.schema().getLabelId();
            builder.append( format( "NewIndexDescriptorFactory.forLabel( %d, %s )", labelId,
                    asString( descriptor.schema().getPropertyIds() ) ) );
        }
        else if ( arg instanceof LabelSchemaDescriptor )
        {
            LabelSchemaDescriptor descriptor = (LabelSchemaDescriptor) arg;
            int labelId = descriptor.getLabelId();
            builder.append( format( "SchemaDescriptorFactory.forLabel( %d, %s )", labelId,
                    asString( descriptor.getPropertyIds() ) ) );
        }
        else if ( arg instanceof UniquenessConstraint )
        {
            UniquenessConstraint constraint = (UniquenessConstraint) arg;
            int labelId = constraint.label();
            builder.append( format( "new UniquenessConstraint( SchemaDescriptorFactory.forLabel( %d, %s ) )", labelId,
                    asString( constraint.descriptor().getPropertyIds() ) ) );
        }
        else if ( arg instanceof NodePropertyExistenceConstraint )
        {
            NodePropertyExistenceConstraint constraint = (NodePropertyExistenceConstraint) arg;
            int labelId = constraint.label();
            builder.append( format( "new NodePropertyExistenceConstraint( SchemaDescriptorFactory.forLabel( %d, %s ) )",
                    labelId, asString( constraint.descriptor().getPropertyIds() ) ) );
        }
        else if ( arg instanceof RelationshipPropertyExistenceConstraint )
        {
            RelationTypeSchemaDescriptor descriptor = (RelationTypeSchemaDescriptor) (
                    (RelationshipPropertyExistenceConstraint) arg)
                    .descriptor();
            int relTypeId = descriptor.getRelTypeId();
            builder.append(
                    format( "new RelationshipPropertyExistenceConstraint( SchemaDescriptorFactory.forLabel( %d, %s ) )",
                            relTypeId, asString( descriptor.getPropertyIds() ) ) );
        }
        else
        {
            throw new IllegalArgumentException(
                    format( "Can't handle argument of type: %s with value: %s", arg.getClass(), arg ) );
        }
    }

    private String asString( int[] propertyIds )
    {
        List<String> strings = Arrays.stream( propertyIds ).mapToObj( i -> "" + i ).collect( Collectors.toList() );
        return String.join( ",", strings );
    }
}
