/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.com.storecopy;

import java.io.IOException;

import org.neo4j.com.Response;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;

public interface ResponseUnpacker
{
    <T> T unpackResponse( Response<T> response ) throws IOException;

    <T> T unpackResponse( Response<T> response, TxHandler handler ) throws IOException;

    public static abstract class Adapter implements ResponseUnpacker
    {
        @Override
        public <T> T unpackResponse( Response<T> response ) throws IOException
        {
            return unpackResponse( response, NO_ACTION );
        }
    }

    public interface TxHandler
    {
        void accept( CommittedTransactionRepresentation tx );

        void done();
    }

    public static final TxHandler NO_ACTION = new TxHandler()
    {
        @Override
        public void accept( CommittedTransactionRepresentation tx )
        {   // Do nothing
        }

        @Override
        public void done()
        {   // Do nothing
        }
    };
}
