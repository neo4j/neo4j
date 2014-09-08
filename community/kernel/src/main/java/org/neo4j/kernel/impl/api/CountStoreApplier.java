/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.store.CountsStore;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandHandler;

import static org.neo4j.kernel.api.ReadOperations.ANY_LABEL;

public class CountStoreApplier extends NeoCommandHandler.Adapter
{
    private final CountsStore countsStore;
    private int nodesDelta;

    public CountStoreApplier( CountsStore countsStore )
    {
        this.countsStore = countsStore;
    }

    @Override
    public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
    {
        if ( !command.getBefore().inUse() && command.getAfter().inUse() )
        { // node added
            nodesDelta++;
        }
        else if ( command.getBefore().inUse() && !command.getAfter().inUse() )
        { // node deleted
            nodesDelta--;
        }
        return true;
    }

    @Override
    public void apply()
    {
        countsStore.updateCountsForNode( ANY_LABEL, nodesDelta );
    }
}
