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

import org.neo4j.common.EntityType;
import org.neo4j.internal.helpers.Strings;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.NodeExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.NodeKeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;

import static java.lang.String.format;

public enum DbStructureArgumentFormatter implements ArgumentFormatter
{
    INSTANCE;

    private static final List<String> IMPORTS = Arrays.asList(
            ConstraintDescriptorFactory.class.getCanonicalName(),
            UniquenessConstraintDescriptor.class.getCanonicalName(),
            RelExistenceConstraintDescriptor.class.getCanonicalName(),
            NodeExistenceConstraintDescriptor.class.getCanonicalName(),
            NodeKeyConstraintDescriptor.class.getCanonicalName(),
            SchemaDescriptor.class.getCanonicalName(),
            IndexDescriptor.class.getCanonicalName(),
            IndexPrototype.class.getCanonicalName()
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
        else if ( arg instanceof IndexDescriptor )
        {
            IndexDescriptor descriptor = (IndexDescriptor) arg;
            String className = IndexPrototype.class.getSimpleName();
            SchemaDescriptor schema = descriptor.schema();
            String methodName = !descriptor.isUnique() ? "forSchema" : "uniqueForSchema";
            builder.append( String.format( "%s.%s( ", className, methodName));
            formatArgument( builder, schema );
            builder.append( " ).withName( \"" ).append( descriptor.getName() ).append( "\" )" );
            builder.append( ".materialise( " ).append( String.valueOf( descriptor.getId() ) ).append( " )" );
        }
        else if ( arg instanceof SchemaDescriptor )
        {
            SchemaDescriptor schema = (SchemaDescriptor) arg;
            if ( schema.isLabelSchemaDescriptor() )
            {
                LabelSchemaDescriptor descriptor = schema.asLabelSchemaDescriptor();
                String className = SchemaDescriptor.class.getSimpleName();
                int labelId = descriptor.getLabelId();
                builder.append( format( "%s.forLabel( %d, %s )",
                        className, labelId, asString( descriptor.getPropertyIds() ) ) );
            }
            else if ( schema.isRelationshipTypeSchemaDescriptor() )
            {
                RelationTypeSchemaDescriptor descriptor = schema.asRelationshipTypeSchemaDescriptor();
                String className = SchemaDescriptor.class.getSimpleName();
                int labelId = descriptor.getRelTypeId();
                builder.append( format( "%s.forRelType( %d, %s )",
                        className, labelId, asString( descriptor.getPropertyIds() ) ) );
            }
            else if ( schema.isFulltextSchemaDescriptor() )
            {
                FulltextSchemaDescriptor descriptor = schema.asFulltextSchemaDescriptor();
                String className = SchemaDescriptor.class.getSimpleName();
                int[] entityIds = descriptor.getEntityTokenIds();
                builder.append( format( "%s.fulltext( EntityType.%s, IndexConfig.empty(), new int[] {%s}, new int[] {%s} )",
                        className, descriptor.entityType().name(), asString( entityIds ), asString( descriptor.getPropertyIds() ) ) );
            }
            else
            {
                throw new IllegalArgumentException(
                        format( "Can't handle argument of type: %s with value: %s", arg.getClass(), arg ) );
            }
        }
        else if ( arg instanceof ConstraintDescriptor )
        {
            ConstraintDescriptor constraint = (ConstraintDescriptor) arg;
            EntityType entityType = constraint.schema().entityType();
            if ( constraint.enforcesUniqueness() && !constraint.enforcesPropertyExistence() && entityType == EntityType.NODE )
            {
                String className = ConstraintDescriptorFactory.class.getSimpleName();
                int labelId = constraint.schema().getLabelId();
                builder.append( format( "%s.uniqueForLabel( %d, %s )",
                        className, labelId, asString( constraint.schema().getPropertyIds() ) ) );
            }
            else if ( !constraint.enforcesUniqueness() && constraint.enforcesPropertyExistence() && entityType == EntityType.NODE )
            {
                String className = ConstraintDescriptorFactory.class.getSimpleName();
                int labelId = constraint.schema().getLabelId();
                builder.append( format( "%s.existsForLabel( %d, %s )",
                        className, labelId, asString( constraint.schema().getPropertyIds() ) ) );
            }
            else if ( !constraint.enforcesUniqueness() && constraint.enforcesPropertyExistence() && entityType == EntityType.RELATIONSHIP )
            {
                SchemaDescriptor descriptor = constraint.schema();
                String className = ConstraintDescriptorFactory.class.getSimpleName();
                int relTypeId = descriptor.getRelTypeId();
                builder.append( format( "%s.existsForReltype( %d, %s )",
                        className, relTypeId, asString( descriptor.getPropertyIds() ) ) );
            }
            else if ( constraint.enforcesUniqueness() && constraint.enforcesPropertyExistence() && entityType == EntityType.NODE )
            {
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
        else
        {
            throw new IllegalArgumentException(
                    format( "Can't handle argument of type: %s with value: %s", arg.getClass(), arg ) );
        }
    }

    private static String asString( int[] propertyIds )
    {
        List<String> strings = Arrays.stream( propertyIds ).mapToObj( i -> "" + i ).collect( Collectors.toList() );
        return String.join( ", ", strings );
    }
}
