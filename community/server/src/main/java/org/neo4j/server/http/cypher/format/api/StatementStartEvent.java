/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.http.cypher.format.api;

import java.util.List;

/**
 * An event that marks a start of statement's execution output stream.
 */
public class StatementStartEvent implements OutputEvent
{

    private final List<String> columns;
    private final Statement statement;

    public StatementStartEvent( Statement statement, List<String> columns )
    {
        this.statement = statement;
        this.columns = columns;
    }

    @Override
    public Type getType()
    {
        return Type.STATEMENT_START;
    }

    public Statement getStatement()
    {
        return statement;
    }

    public List<String> getColumns()
    {
        return columns;
    }
}
