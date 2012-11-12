/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.rest.domain;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

/*
 * Maybe refactor to have a constructor with default stuff and make
 * methods non-static
 */
public class TraversalDescriptionBuilder
{
    public static TraversalDescription from( Map<String, Object> description )
    {
        try
        {
            TraversalDescription result = Traversal.description();
            result = describeOrder( result, description );
            result = describeUniqueness( result, description );
            result = describeRelationships( result, description );
            result = describePruneEvaluator( result, description );
            result = describeReturnFilter( result, description );
            return result;
        }
        catch ( NoClassDefFoundError e )
        {
            // This one can happen if you run on Java 5, but haven't included
            // the
            // backported javax.script jar file(s) on the classpath.
            throw new EvaluationException( e );
        }
    }

    @SuppressWarnings( "unchecked" )
    private static TraversalDescription describeReturnFilter( TraversalDescription result,
            Map<String, Object> description )
    {
        Object returnDescription = description.get( "return_filter" );
        if ( returnDescription != null )
        {
            result = result.filter( EvaluatorFactory.returnFilter( (Map) returnDescription ) );
        }
        else
        {
            // Default return evaluator
            result = result.filter( Traversal.returnAllButStartNode() );
        }
        return result;
    }

    @SuppressWarnings( "unchecked" )
    private static TraversalDescription describePruneEvaluator( TraversalDescription result,
            Map<String, Object> description )
    {
        Object pruneDescription = description.get( "prune_evaluator" );
        if ( pruneDescription != null )
        {
            result = result.prune( EvaluatorFactory.pruneEvaluator( (Map) pruneDescription ) );
        }

        Object maxDepth = description.get( "max_depth" );
        maxDepth = maxDepth != null || pruneDescription != null ? maxDepth : 1;
        if ( maxDepth != null )
        {
            result = result.prune( Traversal.pruneAfterDepth( ( (Number) maxDepth ).intValue() ) );
        }
        return result;
    }

    @SuppressWarnings( "unchecked" )
    private static TraversalDescription describeRelationships( TraversalDescription result,
            Map<String, Object> description )
    {
        Object relationshipsDescription = description.get( "relationships" );
        if ( relationshipsDescription != null )
        {
            Collection<Object> pairDescriptions;
            if ( relationshipsDescription instanceof Collection )
            {
                pairDescriptions = (Collection<Object>) relationshipsDescription;
            }
            else
            {
                pairDescriptions = Arrays.asList( relationshipsDescription );
            }

            for ( Object pairDescription : pairDescriptions )
            {
                Map map = (Map) pairDescription;
                String name = (String) map.get( "type" );
                RelationshipType type = DynamicRelationshipType.withName( name );
                String directionName = (String) map.get( "direction" );
                result = directionName == null ? result.relationships( type ) : result.relationships( type,
                        stringToEnum( directionName, RelationshipDirection.class, true ).internal );
            }
        }
        return result;
    }

    private static TraversalDescription describeUniqueness( TraversalDescription result, Map<String, Object> description )
    {
        Object uniquenessDescription = description.get( "uniqueness" );
        if ( uniquenessDescription != null )
        {
            String name = null;
            Object value = null;
            if ( uniquenessDescription instanceof Map )
            {
                Map map = (Map) uniquenessDescription;
                name = (String) map.get( "name" );
                value = map.get( "value" );
            }
            else
            {
                name = (String) uniquenessDescription;
            }
            org.neo4j.kernel.Uniqueness uniqueness = stringToEnum( enumifyName( name ),
                    org.neo4j.kernel.Uniqueness.class, true );
            result = value == null ? result.uniqueness( uniqueness ) : result.uniqueness( uniqueness, value );
        }
        return result;
    }

    private static TraversalDescription describeOrder( TraversalDescription result, Map<String, Object> description )
    {
        String orderDescription = (String) description.get( "order" );
        if ( orderDescription != null )
        {
            Order order = stringToEnum( enumifyName( orderDescription ), Order.class, true );
            // TODO Fix
            switch ( order )
            {
            case BREADTH_FIRST:
                result = result.breadthFirst();
                break;
            case DEPTH_FIRST:
                result = result.depthFirst();
                break;
            }
        }
        return result;
    }

    private static <T extends Enum<T>> T stringToEnum( String name, Class<T> enumClass, boolean fuzzyMatch )
    {
        if ( name == null )
        {
            return null;
        }

        // name = enumifyName( name );
        for ( T candidate : enumClass.getEnumConstants() )
        {
            if ( candidate.name()
                    .equals( name ) )
            {
                return candidate;
            }
        }
        if ( fuzzyMatch )
        {
            for ( T candidate : enumClass.getEnumConstants() )
            {
                if ( candidate.name()
                        .startsWith( name ) )
                {
                    return candidate;
                }
            }
        }
        throw new RuntimeException( "Unregognized " + enumClass.getSimpleName() + " '" + name + "'" );
    }

    private static String enumifyName( String name )
    {
        return name.replaceAll( " ", "_" )
                .toUpperCase();
    }
}
