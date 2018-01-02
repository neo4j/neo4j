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
package org.neo4j.kernel.impl.util.dbstructure;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.helpers.Strings;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.RelationshipPropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;

import static java.lang.String.format;

public enum DbStructureArgumentFormatter implements ArgumentFormatter
{
    INSTANCE;

    private static List<String> IMPORTS = Arrays.asList(
            UniquenessConstraint.class.getCanonicalName(),
            IndexDescriptor.class.getCanonicalName()
    );

    @Override
    public Collection<String> imports()
    {
        return IMPORTS;
    }

    public void formatArgument( Appendable builder, Object arg ) throws IOException
    {
        if ( arg == null ) {
            builder.append( "null" );
        } else if ( arg instanceof String )
        {
            builder.append( '"' );
            Strings.escape( builder, arg.toString() );
            builder.append( '"' );
        }
        else if ( arg instanceof Long )
        {
            builder.append( arg.toString() );
            builder.append( 'l' );
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
            } else if ( Double.isInfinite( d ) ) {
                builder.append( d < 0 ? "Double.NEGATIVE_INFINITY" : "Double.POSITIVE_INFINITY" );
            } else
            {
                builder.append( arg.toString() );
                builder.append( 'd' );
            }
        }
        else if ( arg instanceof IndexDescriptor )
        {
            IndexDescriptor descriptor = (IndexDescriptor) arg;
            int labelId = descriptor.getLabelId();
            int propertyKeyId = descriptor.getPropertyKeyId();
            builder.append( format( "new IndexDescriptor( %s, %s )", labelId, propertyKeyId ) );
        }
        else if ( arg instanceof UniquenessConstraint )
        {
            UniquenessConstraint constraint = (UniquenessConstraint) arg;
            int labelId = constraint.label();
            int propertyKeyId = constraint.propertyKey();
            builder.append( format( "new UniquenessConstraint( %s, %s )", labelId, propertyKeyId ) );
        }
        else if ( arg instanceof NodePropertyExistenceConstraint )
        {
            NodePropertyExistenceConstraint constraint = (NodePropertyExistenceConstraint) arg;
            int labelId = constraint.label();
            int propertyKeyId = constraint.propertyKey();
            builder.append( format( "new NodePropertyExistenceConstraint( %s, %s )", labelId, propertyKeyId ) );
        }
        else if ( arg instanceof RelationshipPropertyExistenceConstraint )
        {
            RelationshipPropertyExistenceConstraint constraint = (RelationshipPropertyExistenceConstraint) arg;
            int relTypeId = constraint.relationshipType();
            int propertyKeyId = constraint.propertyKey();
            builder.append( format( "new RelationshipPropertyExistenceConstraint( %s, %s )", relTypeId, propertyKeyId ) );
        }
        else
        {
            throw new IllegalArgumentException( format(
                "Can't handle argument of type: %s with value: %s", arg.getClass(), arg
            ) );
        }
    }
}
