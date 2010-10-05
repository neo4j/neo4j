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

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.PruneEvaluator;

class MultiPruneEvaluator implements PruneEvaluator
{
    private final PruneEvaluator[] prunings;

    MultiPruneEvaluator( PruneEvaluator... prunings )
    {
        this.prunings = prunings;
    }

    public boolean pruneAfter( Path position )
    {
        for ( PruneEvaluator pruner : this.prunings )
        {
            if ( pruner.pruneAfter( position ) )
            {
                return true;
            }
        }
        return false;
    }

    public MultiPruneEvaluator add( PruneEvaluator pruner )
    {
        PruneEvaluator[] newPrunings = new PruneEvaluator[this.prunings.length+1];
        System.arraycopy( this.prunings, 0, newPrunings, 0, this.prunings.length );
        newPrunings[newPrunings.length-1] = pruner;
        return new MultiPruneEvaluator( newPrunings );
    }
}
