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
package org.neo4j.ndp.runtime.internal;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.ndp.runtime.spi.RecordStream;
import org.neo4j.ndp.runtime.spi.StatementRunner;

public class CypherStatementRunner implements StatementRunner
{
    private final GraphDatabaseService db;

    public CypherStatementRunner( GraphDatabaseService db )
    {
        this.db = db;
    }

    @Override
    public RecordStream run( final SessionState ctx, final String statement,
            final Map<String,Object> params ) throws KernelException
    {
        // Temporary until we move parsing to cypher, or run a parser up here
        if ( statement.equalsIgnoreCase( "begin" ) )
        {
            ctx.beginTransaction();
            return RecordStream.EMPTY;
        }
        else if ( statement.equalsIgnoreCase( "commit" ) )
        {
            ctx.commitTransaction();
            return RecordStream.EMPTY;
        }
        else if ( statement.equalsIgnoreCase( "rollback" ) )
        {
            ctx.rollbackTransaction();
            return RecordStream.EMPTY;
        }
        else if ( statement.equalsIgnoreCase( "foobar" ) )
        {
            throw new RuntimeException("Foobar occurred");
        }
        else
        {
            if ( !ctx.hasTransaction() )
            {
                ctx.beginImplicitTransaction();
            }
            return new CypherAdapterStream( db.execute( statement, params ) );
        }
    }
}
