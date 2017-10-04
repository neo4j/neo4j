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
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;

import static java.lang.String.format;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.GENERAL;

public enum DbStructureArgumentFormatter implements ArgumentFormatter
{
    INSTANCE;

    private static List<String> IMPORTS = Arrays.asList(
            ConstraintDescriptorFactory.class.getCanonicalName(),
            UniquenessConstraintDescriptor.class.getCanonicalName(),
            RelExistenceConstraintDescriptor.class.getCanonicalName(),
            NodeExistenceConstraintDescriptor.class.getCanonicalName(),
            NodeKeyConstraintDescriptor.class.getCanonicalName(),
            LabelSchemaDescriptor.class.getCanonicalName(),
            SchemaDescriptorFactory.class.getCanonicalName(),
            IndexDescriptor.class.getCanonicalName(),
            IndexDescriptorFactory.class.getCanonicalName()
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
        else if ( arg instanceof IndexDescriptor )
        {
            IndexDescriptor descriptor = (IndexDescriptor) arg;
            int labelId = descriptor.schema().getLabelId();
            String methodName = descriptor.type() == GENERAL ? "forLabel" : "uniqueForLabel";
            builder.append( format( "IndexDescriptorFactory.%s( %d, %s )", methodName,
                    labelId, asString( descriptor.schema().getPropertyIds() ) ) );
        }
        else if ( arg instanceof LabelSchemaDescriptor )
        {
            LabelSchemaDescriptor descriptor = (LabelSchemaDescriptor) arg;
            int labelId = descriptor.getLabelId();
            builder.append( format( "SchemaDescriptorFactory.forLabel( %d, %s )", labelId,
                    asString( descriptor.getPropertyIds() ) ) );
        }
        else if ( arg instanceof UniquenessConstraintDescriptor )
        {
            UniquenessConstraintDescriptor constraint = (UniquenessConstraintDescriptor) arg;
            int labelId = constraint.schema().getLabelId();
            builder.append( format( "ConstraintDescriptorFactory.uniqueForLabel( %d, %s )",
                    labelId,
                    asString( constraint.schema().getPropertyIds() ) ) );
        }
        else if ( arg instanceof NodeExistenceConstraintDescriptor )
        {
            NodeExistenceConstraintDescriptor constraint = (NodeExistenceConstraintDescriptor) arg;
            int labelId = constraint.schema().getLabelId();
            builder.append( format( "ConstraintDescriptorFactory.existsForLabel( %d, %s )",
                    labelId, asString( constraint.schema().getPropertyIds() ) ) );
        }
        else if ( arg instanceof RelExistenceConstraintDescriptor )
        {
            RelationTypeSchemaDescriptor descriptor = ((RelExistenceConstraintDescriptor) arg).schema();
            int relTypeId = descriptor.getRelTypeId();
            builder.append( format( "ConstraintDescriptorFactory.existsForReltype( %d, %s )", relTypeId,
                    asString( descriptor.getPropertyIds() ) ) );
        }
        else if ( arg instanceof NodeKeyConstraintDescriptor )
        {
            NodeKeyConstraintDescriptor constraint = (NodeKeyConstraintDescriptor) arg;
            int labelId = constraint.schema().getLabelId();
            builder.append( format( "ConstraintDescriptorFactory.nodeKeyForLabel( %d, %s )",
                    labelId,
                    asString( constraint.schema().getPropertyIds() ) ) );
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
        return String.join( ", ", strings );
    }
}
