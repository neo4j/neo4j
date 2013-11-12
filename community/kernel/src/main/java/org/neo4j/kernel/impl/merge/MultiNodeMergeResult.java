/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.merge;

import java.util.NoSuchElementException;

import org.neo4j.graphdb.MergeResult;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.core.NodeManager;

public class MultiNodeMergeResult implements MergeResult<Node>
{
    private Statement statement;

    private final NodeManager nodeManager;
    private final PrimitiveLongIterator iterator;

    private Node next;

    public MultiNodeMergeResult( Statement statement,
                                 NodeManager nodeManager,
                                 PrimitiveLongIterator iterator )
    {
        this.statement = statement;
        this.nodeManager = nodeManager;
        this.iterator = iterator;

        computeNext();
    }

    @Override
    public Node single()
    {
        Node result = next();

        if ( hasNext() )
        {
            throw new NoSuchElementException();
        }

        return result;
    }

    @Override
    public boolean hasNext()
    {
        return null != next;
    }

    @Override
    public Node next()
    {
        if ( hasNext() )
        {
            Node result = next;
            computeNext();
            return result;
        }
        else
        {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsNewlyCreated()
    {
        return false;
    }

    @Override
    public void close()
    {
        if ( null == statement )
        {
            throw new IllegalStateException( "Cannot close merge result twice" );
        }

        statement.close();
        statement = null;
    }

    private void computeNext()
    {
        statement.assertOpen();
        if ( iterator.hasNext() )
        {
            next = nodeManager.newNodeProxyById( iterator.next() );
        }
        else
        {
            next = null;
            statement.close();
        }
    }
}
