/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.StandardExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public final class TraversalDescriptionImpl implements TraversalDescription
{
    public TraversalDescriptionImpl()
    {
        this( StandardExpander.DEFAULT, Uniqueness.NODE_GLOBAL, null,
                PruneEvaluator.NONE, Traversal.returnAll(),
                Traversal.preorderDepthFirst() );
    }

    final Expander expander;
    final UniquenessFactory uniqueness;
    final Object uniquenessParameter;
    final PruneEvaluator pruning;
    final Predicate<Path> filter;
    final BranchOrderingPolicy branchSelector;

    private TraversalDescriptionImpl( Expander expander,
            UniquenessFactory uniqueness, Object uniquenessParameter,
            PruneEvaluator pruning, Predicate<Path> filter,
            BranchOrderingPolicy branchSelector )
    {
        this.expander = expander;
        this.uniqueness = uniqueness;
        this.uniquenessParameter = uniquenessParameter;
        this.pruning = pruning;
        this.filter = filter;
        this.branchSelector = branchSelector;
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#traverse(org.neo4j.graphdb.Node)
     */
    public Traverser traverse( Node startNode )
    {
        return new TraverserImpl( this, startNode );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#uniqueness(org.neo4j.graphdb.traversal.Uniqueness)
     */
    public TraversalDescription uniqueness( UniquenessFactory uniqueness )
    {
        return new TraversalDescriptionImpl( expander, uniqueness, null, pruning,
                filter, branchSelector );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#uniqueness(org.neo4j.graphdb.traversal.Uniqueness, java.lang.Object)
     */
    public TraversalDescription uniqueness( UniquenessFactory uniqueness,
            Object parameter )
    {
        if ( this.uniqueness == uniqueness )
        {
            if ( uniquenessParameter == null ? parameter == null
                    : uniquenessParameter.equals( parameter ) )
            {
                return this;
            }
        }

        return new TraversalDescriptionImpl( expander, uniqueness, parameter,
                pruning, filter, branchSelector );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#prune(org.neo4j.graphdb.traversal.PruneEvaluator)
     */
    public TraversalDescription prune( PruneEvaluator pruning )
    {
        if ( this.pruning == pruning )
        {
            return this;
        }

        nullCheck( pruning, PruneEvaluator.class, "NO_PRUNING" );
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, addPruneEvaluator( pruning ),
                filter, branchSelector );
    }

    private PruneEvaluator addPruneEvaluator( PruneEvaluator pruning )
    {
        if ( this.pruning instanceof MultiPruneEvaluator )
        {
            return ((MultiPruneEvaluator) this.pruning).add( pruning );
        }
        else
        {
            if ( this.pruning == PruneEvaluator.NONE )
            {
                return pruning;
            }
            else
            {
                return new MultiPruneEvaluator( new PruneEvaluator[] {
                        this.pruning, pruning } );
            }
        }
    }

    public TraversalDescription filter( Predicate<Path> filter )
    {
        if ( this.filter == filter )
        {
            return this;
        }

        if ( filter == null )
        {
            throw new IllegalArgumentException( "Return filter may not be null, " +
            		"use " + Traversal.class.getSimpleName() + ".returnAll() instead." );
        }
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, pruning, addFilter( filter ), branchSelector );
    }

    private Predicate<Path> addFilter( Predicate<Path> filter )
    {
        if ( this.filter instanceof MultiFilter )
        {
            return ((MultiFilter) this.pruning).add( filter );
        }
        else
        {
            if ( this.filter == Traversal.returnAll() )
            {
                return filter;
            }
            else
            {
                return new MultiFilter( new Predicate[] {
                        this.filter, filter } );
            }
        }
    }
    
    private static <T> void nullCheck( T parameter, Class<T> parameterType,
            String defaultName )
    {
        if ( parameter == null )
        {
            String typeName = parameterType.getSimpleName();
            throw new IllegalArgumentException( typeName
                                                + " may not be null, use "
                                                + typeName + "." + defaultName
                                                + " instead." );
        }
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#order(org.neo4j.graphdb.traversal.Order)
     */
    public TraversalDescription order( BranchOrderingPolicy selector )
    {
        if ( this.branchSelector == selector )
        {
            return this;
        }
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, pruning, filter, selector );
    }

    public TraversalDescription depthFirst()
    {
        return order( Traversal.preorderDepthFirst() );
    }

    public TraversalDescription breadthFirst()
    {
        return order( Traversal.preorderBreadthFirst() );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#relationships(org.neo4j.graphdb.RelationshipType)
     */
    public TraversalDescription relationships( RelationshipType type )
    {
        return relationships( type, Direction.BOTH );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#relationships(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)
     */
    public TraversalDescription relationships( RelationshipType type,
            Direction direction )
    {
        return expand( expander.add( type, direction ) );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#expand(org.neo4j.graphdb.RelationshipExpander)
     */
    public TraversalDescription expand(RelationshipExpander expander)
    {
        if ( expander.equals( this.expander ) )
        {
            return this;
        }
        return new TraversalDescriptionImpl(
                Traversal.expander( expander ), uniqueness,
                uniquenessParameter, pruning, filter, branchSelector );
    }
}
