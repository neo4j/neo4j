/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest.transactional;

import static org.neo4j.helpers.collection.IteratorUtil.iterator;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.server.rest.transactional.Statement;
import org.neo4j.server.rest.transactional.StatementDeserializer;
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
