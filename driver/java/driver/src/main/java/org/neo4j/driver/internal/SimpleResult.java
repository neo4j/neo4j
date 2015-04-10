/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal;

import java.util.Iterator;
import java.util.List;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.ReusableResult;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;

public class SimpleResult implements Result
{
    private final Iterable<String> fieldNames;
    private final List<Record> body;
    private final Iterator<Record> iter;
    private Record current;

    public SimpleResult( Iterable<String> fieldNames, List<Record> body )
    {
        this.fieldNames = fieldNames;
        this.body = body;
        this.iter = body.iterator();
    }

    @Override
    public ReusableResult retain()
    {
        return new StandardReusableResult( body );
    }

    @Override
    public Record single()
    {
        return iter.next();
    }

    @Override
    public boolean next()
    {
        if ( iter.hasNext() )
        {
            current = iter.next();
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public Value get( int fieldIndex )
    {
        return current.get( fieldIndex );
    }

    @Override
    public Value get( String fieldName )
    {
        return current.get( fieldName );
    }

    @Override
    public Iterable<String> fieldNames()
    {
        return fieldNames;
    }

    private static class StandardReusableResult implements ReusableResult
    {
        private final List<Record> body;

        private StandardReusableResult( List<Record> body )
        {
            this.body = body;
        }

        @Override
        public long size()
        {
            return body.size();
        }

        @Override
        public Record get( long index )
        {
            if ( index < 0 || index >= body.size() )
            {
                throw new ClientException( "Value " + index + " does not exist" );
            }
            return body.get( (int) index );
        }

        @Override
        public Iterator<Record> iterator()
        {
            return body.iterator();
        }
    }
}
