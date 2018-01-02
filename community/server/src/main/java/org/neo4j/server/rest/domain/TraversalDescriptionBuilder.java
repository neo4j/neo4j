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
package org.neo4j.server.rest.domain;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.OrderedByTypeExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

import static org.neo4j.graphdb.traversal.Evaluators.excludeStartPosition;

public class TraversalDescriptionBuilder
{

    private final EvaluatorFactory evaluatorFactory;

    public TraversalDescriptionBuilder( boolean enableSandboxing )
    {
        this.evaluatorFactory = new EvaluatorFactory( enableSandboxing );
    }

    public TraversalDescription from( Map<String, Object> description )
    {
        try
        {
            TraversalDescription result = new MonoDirectionalTraversalDescription();
            result = describeOrder( result, description );
            result = describeUniqueness( result, description );
            result = describeExpander( result, description );
            result = describePruneEvaluator( result, description );
            result = describeReturnFilter( result, description );
            return result;
        }
        catch ( NoClassDefFoundError e )
        {
            // This one can happen if you run on Java 5, but haven't included
            // the backported javax.script jar file(s) on the classpath.
            throw new EvaluationException( e );
        }
    }

    @SuppressWarnings( "unchecked" )
    private TraversalDescription describeReturnFilter( TraversalDescription result,
            Map<String, Object> description )
    {
        Object returnDescription = description.get( "return_filter" );
        if ( returnDescription != null )
        {
            Evaluator filter = evaluatorFactory.returnFilter( (Map) returnDescription );
            // Filter is null when "all" is used, no filter then
            if ( filter != null )
            {
                result = result.evaluator( filter );
            }
        }
        else
        {
            // Default return evaluator
            result = result.evaluator( excludeStartPosition() );
        }
        return result;
    }

    @SuppressWarnings( "unchecked" )
    private TraversalDescription describePruneEvaluator( TraversalDescription result,
            Map<String, Object> description )
    {
        Object pruneDescription = description.get( "prune_evaluator" );
        if ( pruneDescription != null )
        {
            Evaluator pruner = evaluatorFactory.pruneEvaluator( (Map) pruneDescription );
            if ( pruner != null )
            {
                result = result.evaluator( pruner );
            }
        }

        Object maxDepth = description.get( "max_depth" );
        maxDepth = maxDepth != null || pruneDescription != null ? maxDepth : 1;
        if ( maxDepth != null )
        {
            result = result.evaluator( Evaluators.toDepth( ((Number) maxDepth).intValue() ) );
        }
        return result;
    }

    @SuppressWarnings( "unchecked" )
    private TraversalDescription describeExpander( TraversalDescription result,
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

            Expander expander = createExpander( description );

            for ( Object pairDescription : pairDescriptions )
            {
                Map map = (Map) pairDescription;
                String name = (String) map.get( "type" );
                RelationshipType type = DynamicRelationshipType.withName( name );
                String directionName = (String) map.get( "direction" );
                expander = directionName == null ? expander.add( type ) :
                        expander.add( type, stringToEnum( directionName,
                                RelationshipDirection.class, true ).internal );
            }
            result = result.expand( expander );
        }
        return result;
    }

    private Expander createExpander( Map<String, Object> description )
    {
        if(description.containsKey( "expander" ))
        {
            Object expanderDesc = description.get( "expander" );
            if(! (expanderDesc instanceof String))
            {
                throw new IllegalArgumentException( "Invalid expander type '"+expanderDesc+"', expected a string name." );

            }

            String expanderName = (String) expanderDesc;
            if(expanderName.equalsIgnoreCase( "order_by_type" ))
            {
                return new OrderedByTypeExpander();
            }

            throw new IllegalArgumentException( "Unknown expander type: '"+expanderName+"'" );
        }

        // Default expander
        return Traversal.emptyExpander();
    }

    private TraversalDescription describeUniqueness( TraversalDescription result, Map<String, Object> description )
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

    private TraversalDescription describeOrder( TraversalDescription result, Map<String, Object> description )
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

    private <T extends Enum<T>> T stringToEnum( String name, Class<T> enumClass, boolean fuzzyMatch )
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

    private String enumifyName( String name )
    {
        return name.replaceAll( " ", "_" )
                .toUpperCase();
    }
}
