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

import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;

public class FakeTransaction implements Transaction
{
    @Override
    public void commit()
    {

    }

    @Override
    public void rollback()
    {

    }

    @Override
    public boolean isOpen()
    {
        return true;
    }

    @Override
    public void close()
    {

    }

    @Override
    public Result run( String query, Value parameters )
    {
        return null;
    }

    @Override
    public Result run( String query, Map<String, Object> parameters )
    {
        return null;
    }

    @Override
    public Result run( String query, Record parameters )
    {
        return null;
    }

    @Override
    public Result run( String query )
    {
        return null;
    }

    @Override
    public Result run( Query query )
    {
        return null;
    }
}
