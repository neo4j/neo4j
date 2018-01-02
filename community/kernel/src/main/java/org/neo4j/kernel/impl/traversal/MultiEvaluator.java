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
package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.PathEvaluator;

/**
 * Evaluator which can hold multiple {@link Evaluator}s and delegate to them
 * all for evaluation requests.
 */
public class MultiEvaluator<STATE> extends PathEvaluator.Adapter<STATE>
{
    private final PathEvaluator[] evaluators;

    MultiEvaluator( PathEvaluator... evaluators )
    {
        this.evaluators = evaluators;
    }

    /**
     * Returns whether or not the {@code position} is to be included and also
     * if it's going to be continued.
     * 
     * The include/exclude part of the returned {@link Evaluation} will be
     * {@code include} if all of the internal evaluators think it's going to be
     * included, otherwise it will be excluded.
     * 
     * The continue/prune part of the returned {@link Evaluation} will be
     * {@code continue} if all of the internal evaluators think it's going to be
     * continued, otherwise it will be pruned.
     * 
     * @param position the {@link Path} to evaluate.
     * @see Evaluator
     */
    public Evaluation evaluate( Path position, BranchState<STATE> state )
    {
        boolean includes = true;
        boolean continues = true;
        for ( PathEvaluator<STATE> evaluator : this.evaluators )
        {
            Evaluation bla = evaluator.evaluate( position, state );
            if ( !bla.includes() )
            {
                includes = false;
                if ( !continues )
                    return Evaluation.EXCLUDE_AND_PRUNE;
            }
            if ( !bla.continues() )
            {
                continues = false;
                if ( !includes )
                    return Evaluation.EXCLUDE_AND_PRUNE;
            }
        }
        return Evaluation.of( includes, continues );
    }

    /**
     * Adds {@code evaluator} to the list of evaluators wrapped by the returned
     * evaluator. A new {@link MultiEvaluator} instance additionally containing
     * the supplied {@code evaluator} is returned and this instance will be
     * left intact.
     * 
     * @param evaluator the {@link Evaluator} to add to this multi evaluator.
     * @return a new instance containing the current list of evaluator plus
     * the supplied one.
     */
    public MultiEvaluator<STATE> add( PathEvaluator<STATE> evaluator )
    {
        PathEvaluator[] newArray = new PathEvaluator[this.evaluators.length+1];
        System.arraycopy( this.evaluators, 0, newArray, 0, this.evaluators.length );
        newArray[newArray.length-1] = evaluator;
        return new MultiEvaluator<STATE>( newArray );
    }
}
