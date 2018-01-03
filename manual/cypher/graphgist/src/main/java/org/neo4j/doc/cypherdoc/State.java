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
package org.neo4j.doc.cypherdoc;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.internal.DocsExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

class State
{
    final DocsExecutionEngine engine;
    final GraphDatabaseService database;
    final Connection sqlDatabase;
    final File parentDirectory;
    final String url;
    final List<String> knownFiles = new ArrayList<>();
    final Map<String, Object> parameters = new HashMap<>();

    Result latestResult;
    Result testedResult;
    Result latestSqlResult;
    Result testedSqlResult;

    State( DocsExecutionEngine engine, GraphDatabaseService database, Connection sqlConnection,
            File parentDirectory,
            String url )
    {
        this.engine = engine;
        this.database = database;
        this.sqlDatabase = sqlConnection;
        this.parentDirectory = parentDirectory;
        this.url = url.endsWith( "/" ) ? url : url + "/";
    }
}
