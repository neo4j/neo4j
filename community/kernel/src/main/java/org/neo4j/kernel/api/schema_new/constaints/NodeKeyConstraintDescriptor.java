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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema_new.SchemaUtil;

/**
 * This class represents the NODE KEY constraints, internally is enforced by a composite uniqueness constraint and
 * one existence constraint per property. This descriptor should only be used to drop and list the NODE KEY
 * constraint.
 */
public class NodeKeyConstraintDescriptor extends ConstraintDescriptor implements LabelSchemaDescriptor.Supplier
{
    private final LabelSchemaDescriptor schema;
    private final UniquenessConstraintDescriptor uniquenessConstraint;
    private final NodeExistenceConstraintDescriptor[] existenceConstraints;

    NodeKeyConstraintDescriptor( LabelSchemaDescriptor schema )
    {
        super( Type.NODE_KEY );
        this.schema = schema;
        this.uniquenessConstraint = new UniquenessConstraintDescriptor( schema );
        this.existenceConstraints = new NodeExistenceConstraintDescriptor[schema.getPropertyIds().length];
        for ( int i = 0; i < schema.getPropertyIds().length; i++ )
        {
            this.existenceConstraints[i] = ConstraintDescriptorFactory.existsForSchema(
                    SchemaDescriptorFactory.forLabel( schema.getLabelId(), schema.getPropertyIds()[i] ) );
        }
    }

    @Override
    public LabelSchemaDescriptor schema()
    {
        return schema;
    }

    public UniquenessConstraintDescriptor ownedUniquenessConstraint()
    {
        return uniquenessConstraint;
    }

    public NodeExistenceConstraintDescriptor[] ownedExistenceConstraints()
    {
        return existenceConstraints;
    }

    @Override
    public String prettyPrint( TokenNameLookup tokenNameLookup )
    {
        String labelName = escapeLabelOrRelTyp( tokenNameLookup.labelGetName( schema.getLabelId() ) );
        String nodeName = labelName.toLowerCase();
        String properties = SchemaUtil.niceProperties( tokenNameLookup, schema.getPropertyIds(), nodeName + "." );
        if ( schema.getPropertyIds().length > 1 )
        {
            properties = "(" + properties + ")";
        }
        return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT %s IS NODE KEY", nodeName, labelName, properties );
    }

    /**
     * Method to use when listing constraints, to transform uniqueness and existence constraints into NODE
     * KEY constraints. Constraints that cannot be transformed are returned as unchanged.
     */
    public static Iterator<ConstraintDescriptor> addNodeKeys( Iterator<ConstraintDescriptor> constraints )
    {
        List<UniquenessConstraintDescriptor> allUniqueConstraints = new ArrayList<>();
        Map<SchemaDescriptor,ConstraintDescriptor> allExistenceConstraints = new HashMap<>();
        while ( constraints.hasNext() )
        {
            ConstraintDescriptor constraint = constraints.next();
            if ( constraint.type() == ConstraintDescriptor.Type.UNIQUE )
            {
                allUniqueConstraints.add( (UniquenessConstraintDescriptor) constraint );
            }
            else if ( constraint.type() == ConstraintDescriptor.Type.EXISTS )
            {
                allExistenceConstraints.put( constraint.schema(), constraint );
            }
            else
            {
                throw new IllegalStateException(
                        "NODE KEY constraints are syntactic sugar and should not exist in the SchemaStore" );
            }
        }

        return new Iterator<ConstraintDescriptor>()
        {
            private Iterator<UniquenessConstraintDescriptor> uniquenessIterator = allUniqueConstraints.iterator();

            @Override
            public boolean hasNext()
            {
                return uniquenessIterator.hasNext() || !allExistenceConstraints.isEmpty();
            }

            @Override
            public ConstraintDescriptor next()
            {
                if ( uniquenessIterator.hasNext() )
                {
                    UniquenessConstraintDescriptor constraint = uniquenessIterator.next();
                    NodeKeyConstraintDescriptor nodeKeyConstraintDescriptor =
                            ConstraintDescriptorFactory.nodeKeyForSchema( constraint.schema() );
                    boolean allExistenceConstraintsExists = true;
                    for ( NodeExistenceConstraintDescriptor existenceConstraintDescriptor :
                            nodeKeyConstraintDescriptor
                                    .ownedExistenceConstraints() )
                    {
                        if ( !allExistenceConstraints.containsKey( existenceConstraintDescriptor.schema() ) )
                        {
                            allExistenceConstraintsExists = false;
                        }
                    }
                    if ( allExistenceConstraintsExists )
                    {
                        for ( NodeExistenceConstraintDescriptor existenceConstraintDescriptor :
                                nodeKeyConstraintDescriptor
                                        .ownedExistenceConstraints() )
                        {
                            allExistenceConstraints.remove( existenceConstraintDescriptor.schema() );
                        }
                        return nodeKeyConstraintDescriptor;
                    }
                    else
                    {
                        return constraint;
                    }
                }
                else
                {
                    SchemaDescriptor next = allExistenceConstraints.keySet().iterator().next();
                    return allExistenceConstraints.remove( next );
                }
            }
        };
    }
}
