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

import org.neo4j.graphdb.Result;
import org.neo4j.ndp.runtime.spi.Record;
import org.neo4j.ndp.runtime.spi.RecordStream;

public class CypherAdapterStream implements RecordStream
{
    private final Result delegate;
    private final String[] fieldNames;
    private CypherAdapterRecord currentRecord;

    public CypherAdapterStream( Result delegate )
    {
        this.delegate = delegate;
        this.fieldNames = delegate.columns().toArray( new String[delegate.columns().size()] );
        this.currentRecord = new CypherAdapterRecord( fieldNames );
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public String[] fieldNames()
    {
        return fieldNames;
    }

    @Override
    public void accept( final Visitor visitor ) throws Exception
    {
        delegate.accept( new Result.ResultVisitor<Exception>()
        {
            @Override
            public boolean visit( Result.ResultRow row ) throws Exception
            {
                visitor.visit( currentRecord.reset( row ) );
                return true;
            }
        } );
    }

    private static class CypherAdapterRecord implements Record
    {
        private final Object[] fields; // This exists solely to avoid re-creating a new array for each record
        private final String[] fieldNames;

        private CypherAdapterRecord( String[] fieldNames )
        {
            this.fields = new Object[fieldNames.length];
            this.fieldNames = fieldNames;
        }

        @Override
        public Object[] fields()
        {
            return fields;
        }

        public CypherAdapterRecord reset( Result.ResultRow cypherRecord )
        {
            for ( int i = 0; i < fields.length; i++ )
            {
                fields[i] = cypherRecord.get( fieldNames[i] );
            }
            return this;
        }
    }
}
