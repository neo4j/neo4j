/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.util.function.Function;

import org.neo4j.helpers.ArrayUtil;

/**
 * Common {@link InputEntity} decorators, able to provide defaults or overrides.
 */
public class InputEntityDecorators
{
    /**
     * Ensures that all {@link InputNode input nodes} will at least have the given set of labels.
     */
    public static Function<InputNode,InputNode> additiveLabels( final String[] labelNamesToAdd )
    {
        if ( labelNamesToAdd == null || labelNamesToAdd.length == 0 )
        {
            return value -> value;
        }

        return node -> {
            if ( node.hasLabelField() )
            {
                return node;
            }

            String[] union = ArrayUtil.union( node.labels(), labelNamesToAdd );
            if ( union != node.labels() )
            {
                node.setLabels( union );
            }
            return node;
        };
    }

    /**
     * Ensures that {@link InputRelationship input relationships} without a specified relationship type will get
     * the specified default relationship type.
     */
    public static Function<InputRelationship,InputRelationship> defaultRelationshipType( final String defaultType )
    {
        if ( defaultType == null )
        {
            return value -> value;
        }

        return relationship -> {
            if ( relationship.type() == null && !relationship.hasTypeId() )
            {
                relationship.setType( defaultType );
            }

            return relationship;
        };
    }

    public static <ENTITY extends InputEntity> Function<ENTITY,ENTITY> decorators(
            final Function<ENTITY,ENTITY>... decorators )
    {
        return from -> {
            for ( Function<ENTITY,ENTITY> decorator : decorators )
            {
                from = decorator.apply( from );
            }
            return from;
        };
    }

    public static final Function<InputNode,InputNode> NO_NODE_DECORATOR = value -> value;
    public static final Function<InputRelationship,InputRelationship> NO_RELATIONSHIP_DECORATOR = value -> value;
}
