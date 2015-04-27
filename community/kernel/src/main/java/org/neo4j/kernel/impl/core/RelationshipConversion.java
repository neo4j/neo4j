/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.NoSuchElementException;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.RelationshipVisitor;

public class RelationshipConversion implements RelationshipVisitor<RuntimeException>, ResourceIterator<Relationship>
{
    private final NodeProxy.NodeActions actions;
    Cursor cursor;
    Statement statement;
    private Relationship next;

    public RelationshipConversion( NodeProxy.NodeActions actions )
    {
        this.actions = actions;
    }

    @Override
    public void visit( long relId, int type, long startNode, long endNode ) throws RuntimeException
    {
        next = actions.newRelationshipProxy( relId, startNode, type, endNode );
    }

    @Override
    public boolean hasNext()
    {
        return next != null || cursor.next();
    }

    @Override
    public Relationship next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        Relationship current = next;
        next = null;
        return current;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        cursor.close();
        statement.close();
    }
}
