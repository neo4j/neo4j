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
package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class MultiEvaluator implements Evaluator
{
    private final Evaluator[] evaluators;

    MultiEvaluator( Evaluator... prunings )
    {
        this.evaluators = prunings;
    }

    public Evaluation evaluate( Path position )
    {
        boolean includes = true;
        boolean continues = true;
        for ( Evaluator evaluator : this.evaluators )
        {
            Evaluation bla = evaluator.evaluate( position );
            if ( !bla.includes() )
            {
                includes = false;
            }
            if ( !bla.continues() )
            {
                continues = false;
            }
            if ( !continues && !includes )
            {
                return Evaluation.EXCLUDE_AND_PRUNE;
            }
        }
        return Evaluation.of( includes, continues );
    }

    public MultiEvaluator add( Evaluator evaluator )
    {
        Evaluator[] newArray = new Evaluator[this.evaluators.length+1];
        System.arraycopy( this.evaluators, 0, newArray, 0, this.evaluators.length );
        newArray[newArray.length-1] = evaluator;
        return new MultiEvaluator( newArray );
    }
}
