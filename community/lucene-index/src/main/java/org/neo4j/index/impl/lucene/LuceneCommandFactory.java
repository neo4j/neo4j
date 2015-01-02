/**
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
package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;

/**
 * A command factory used for reading lucene specific commands out of an XaLogicalLog.
 */
public class LuceneCommandFactory extends XaCommandFactory
{
    private LuceneDataSource luceneDataSource;

    public LuceneCommandFactory( LuceneDataSource luceneDataSource )
    {
        super();
        this.luceneDataSource = luceneDataSource;
    }

    @Override
    public XaCommand readCommand( ReadableByteChannel channel,
                                  ByteBuffer buffer ) throws IOException
    {
        return LuceneCommand.readCommand( channel, buffer, luceneDataSource );
    }
}
