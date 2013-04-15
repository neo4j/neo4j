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
package org.neo4j.server.rest.transactional;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.server.rest.transactional.error.Neo4jError;

public class ValidatingResultHandler implements TransactionFacade.ResultHandler
{
    // Used to validate that the contract of this interface is never violated.
    private enum State
    {
        BEFORE_PROLOGUE,
        AFTER_PROLOGUE,
        AFTER_EPILOGUE
    }

    List<ExecutionResult> results = new ArrayList<ExecutionResult>();
    long txId;
    List<Neo4jError> errors = new ArrayList<Neo4jError>();

    private State state = State.BEFORE_PROLOGUE;

    @Override
    public void prologue( long txId )
    {
        prologue();
        this.txId = txId;
    }

    @Override
    public void prologue()
    {
        assertEquals( state, State.BEFORE_PROLOGUE );
        this.state = State.AFTER_PROLOGUE;
    }

    @Override
    public void visitStatementResult( ExecutionResult result ) throws Neo4jError
    {
        assertEquals( state, State.AFTER_PROLOGUE );
        results.add( result );
    }

    @Override
    public void epilogue( Iterator<Neo4jError> errors )
    {
        assertEquals( state, State.AFTER_PROLOGUE );
        state = State.AFTER_EPILOGUE;

        while ( errors.hasNext() )
        {
            this.errors.add( errors.next() );
        }
    }
}
