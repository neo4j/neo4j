/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.util.dbstructure;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.helpers.Strings;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;

import static java.lang.String.format;
import static org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor.Type.GENERAL;

public enum DbStructureArgumentFormatter implements ArgumentFormatter
{
    INSTANCE;

    private static List<String> IMPORTS = Arrays.asList(
            ConstraintDescriptorFactory.class.getCanonicalName(),
            UniquenessConstraintDescriptor.class.getCanonicalName(),
            RelExistenceConstraintDescriptor.class.getCanonicalName(),
            NodeExistenceConstraintDescriptor.class.getCanonicalName(),
            NodeKeyConstraintDescriptor.class.getCanonicalName(),
            SchemaDescriptor.class.getCanonicalName(),
            SchemaDescriptorFactory.class.getCanonicalName(),
            SchemaIndexDescriptor.class.getCanonicalName(),
            SchemaIndexDescriptorFactory.class.getCanonicalName()
    );

    @Override
    public Collection<String> imports()
    {
        return IMPORTS;
    }

    @Override
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
        else if ( arg instanceof SchemaIndexDescriptor )
        {
            SchemaIndexDescriptor descriptor = (SchemaIndexDescriptor) arg;
            String className = SchemaIndexDescriptorFactory.class.getSimpleName();
            int labelId = descriptor.schema().keyId();
            String methodName = descriptor.type() == GENERAL ? "forLabel" : "uniqueForLabel";
            builder.append( format( "%s.%s( %d, %s )",
                    className, methodName, labelId, asString( descriptor.schema().getPropertyIds() ) ) );
        }
        else if ( arg instanceof LabelSchemaDescriptor )
        {
            LabelSchemaDescriptor descriptor = (LabelSchemaDescriptor) arg;
            String className = SchemaDescriptorFactory.class.getSimpleName();
            int labelId = descriptor.getLabelId();
            builder.append( format( "%s.forLabel( %d, %s )",
                    className, labelId, asString( descriptor.getPropertyIds() ) ) );
        }
        else if ( arg instanceof UniquenessConstraintDescriptor )
        {
            UniquenessConstraintDescriptor constraint = (UniquenessConstraintDescriptor) arg;
            String className = ConstraintDescriptorFactory.class.getSimpleName();
            int labelId = constraint.schema().getLabelId();
            builder.append( format( "%s.uniqueForLabel( %d, %s )",
                    className, labelId, asString( constraint.schema().getPropertyIds() ) ) );
        }
        else if ( arg instanceof NodeExistenceConstraintDescriptor )
        {
            NodeExistenceConstraintDescriptor constraint = (NodeExistenceConstraintDescriptor) arg;
            String className = ConstraintDescriptorFactory.class.getSimpleName();
            int labelId = constraint.schema().getLabelId();
            builder.append( format( "%s.existsForLabel( %d, %s )",
                    className, labelId, asString( constraint.schema().getPropertyIds() ) ) );
        }
        else if ( arg instanceof RelExistenceConstraintDescriptor )
        {
            RelationTypeSchemaDescriptor descriptor = ((RelExistenceConstraintDescriptor) arg).schema();
            String className = ConstraintDescriptorFactory.class.getSimpleName();
            int relTypeId = descriptor.getRelTypeId();
            builder.append( format( "%s.existsForReltype( %d, %s )",
                    className, relTypeId, asString( descriptor.getPropertyIds() ) ) );
        }
        else if ( arg instanceof NodeKeyConstraintDescriptor )
        {
            NodeKeyConstraintDescriptor constraint = (NodeKeyConstraintDescriptor) arg;
            String className = ConstraintDescriptorFactory.class.getSimpleName();
            int labelId = constraint.schema().getLabelId();
            builder.append( format( "%s.nodeKeyForLabel( %d, %s )",
                    className, labelId, asString( constraint.schema().getPropertyIds() ) ) );
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
