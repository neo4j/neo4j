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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;

class ExhaustingInputVisitorRunnable implements Runnable
{
    private final InputIterator data;
    private final EntityVisitor visitor;

    public ExhaustingInputVisitorRunnable( InputIterator data, EntityVisitor visitor )
    {
        this.data = data;
        this.visitor = visitor;
    }

    @Override
    public void run()
    {
        try ( InputChunk chunk = data.newChunk() )
        {
            while ( data.next( chunk ) )
            {
                while ( chunk.next( visitor ) );
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
        finally
        {
            visitor.close();
        }
    }
}
