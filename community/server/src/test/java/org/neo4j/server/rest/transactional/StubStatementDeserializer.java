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
package org.neo4j.server.rest.transactional;

import static org.neo4j.helpers.collection.IteratorUtil.iterator;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.server.rest.transactional.error.Neo4jError;

public class StubStatementDeserializer extends StatementDeserializer
{
    private final Iterator<Statement> statements;
    private final Iterator<Neo4jError> errors;

    private boolean hasNext;
    private Statement next;

    public static StubStatementDeserializer statements( Statement... statements )
    {
        return new StubStatementDeserializer( IteratorUtil.<Neo4jError>emptyIterator(), iterator( statements ) );
    }

    public StubStatementDeserializer( Iterator<Neo4jError> errors, Iterator<Statement> statements )
    {
        super( new ByteArrayInputStream( new byte[]{} ) );
        this.statements = statements;
        this.errors = errors;

        computeNext();
    }

    private void computeNext()
    {
        hasNext = statements.hasNext();
        if ( hasNext )
        {
            next = statements.next();
        }
        else
        {
            next = null;
        }
    }

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public Statement peek() {
        if ( hasNext )
        {
            return next;
        }
        else
        {
            throw new NoSuchElementException();
        }
    }

    @Override
    public Statement next()
    {
        Statement result = next;
        computeNext();
        return result;
    }

    @Override
    public Iterator<Neo4jError> errors()
    {
        return errors;
    }
}
