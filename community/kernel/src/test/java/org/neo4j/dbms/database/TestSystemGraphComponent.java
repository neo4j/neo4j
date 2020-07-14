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
package org.neo4j.dbms.database;

import java.util.Optional;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class TestSystemGraphComponent implements SystemGraphComponent
{
    final String component;
    SystemGraphComponent.Status status;
    Exception onInit;
    Exception onMigrate;

    public TestSystemGraphComponent( String component, SystemGraphComponent.Status status, Exception onInit, Exception onMigrate )
    {
        this.component = component;
        this.status = status;
        this.onInit = onInit;
        this.onMigrate = onMigrate;
    }

    @Override
    public String component()
    {
        return component;
    }

    @Override
    public Status detect( Transaction tx )
    {
        return status;
    }

    @Override
    public Optional<Exception> initializeSystemGraph( GraphDatabaseService system )
    {
        if ( status == Status.UNINITIALIZED )
        {
            if ( onInit == null )
            {
                status = Status.CURRENT;
            }
            else
            {
                return Optional.of( onInit );
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<Exception> upgradeToCurrent( GraphDatabaseService system )
    {
        if ( status == Status.REQUIRES_UPGRADE )
        {
            if ( onMigrate == null )
            {
                status = Status.CURRENT;
            }
            else
            {
                return Optional.of( onMigrate );
            }
        }
        return Optional.empty();
    }
}
