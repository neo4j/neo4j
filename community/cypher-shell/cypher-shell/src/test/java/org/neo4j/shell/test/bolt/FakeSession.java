/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.shell.test.bolt;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.TypeSystem;

/**
 * A fake session which returns fake StatementResults
 */
public class FakeSession implements Session
{
    private boolean open = true;

    @Override
    public Transaction beginTransaction()
    {
        return null;
    }

    @Override
    public Transaction beginTransaction( TransactionConfig config )
    {
        return null;
    }

    @Override
    public Transaction beginTransaction( String bookmark )
    {
        return null;
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync()
    {
        return null;
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync( TransactionConfig config )
    {
        return null;
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work )
    {
        return null;
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return null;
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return null;
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return null;
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work )
    {
        return null;
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return null;
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return null;
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return null;
    }

    @Override
    public StatementResult run( String statement, TransactionConfig config )
    {
        return FakeStatementResult.parseStatement( statement );
    }

    @Override
    public StatementResult run( String statement, Map<String, Object> parameters, TransactionConfig config )
    {
        return FakeStatementResult.parseStatement( statement );
    }

    @Override
    public StatementResult run( Statement statement, TransactionConfig config )
    {
        return new FakeStatementResult();
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statement, TransactionConfig config )
    {
        return null;
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statement, Map<String, Object> parameters, TransactionConfig config )
    {
        return null;
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement, TransactionConfig config )
    {
        return null;
    }

    @Override
    public String lastBookmark()
    {
        return null;
    }

    @Override
    public void reset()
    {
    }

    @Override
    public boolean isOpen()
    {
        return open;
    }

    @Override
    public void close()
    {
        open = false;
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        return null;
    }

    @Override
    public StatementResult run( String statementTemplate, Value parameters )
    {
        return FakeStatementResult.parseStatement( statementTemplate );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementTemplate, Value parameters )
    {
        return null;
    }

    @Override
    public StatementResult run( String statementTemplate, Map<String, Object> statementParameters )
    {
        return FakeStatementResult.parseStatement( statementTemplate );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementTemplate,
                                                            Map<String, Object> statementParameters )
    {
        return null;
    }

    @Override
    public StatementResult run( String statementTemplate, Record statementParameters )
    {
        return FakeStatementResult.parseStatement( statementTemplate );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementTemplate, Record statementParameters )
    {
        return null;
    }

    @Override
    public StatementResult run( String statementTemplate )
    {
        return FakeStatementResult.parseStatement( statementTemplate );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statementTemplate )
    {
        return null;
    }

    @Override
    public StatementResult run( Statement statement )
    {
        return new FakeStatementResult();
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        return null;
    }

    @Override
    public TypeSystem typeSystem()
    {
        return null;
    }
}
