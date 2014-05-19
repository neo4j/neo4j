/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Writing groups of commands, in a way that is guaranteed to be recoverable, i.e. consistently readable,
 * in the event of failure.
 */
public interface TransactionAppender
{
    /**
     * @param commands commands to write.
     * @return a {@link Future} where when it is {@link Future#isDone() done} the commands can be read
     * back consistently. The completed future will hand back the transaction id of.
     * @throws IOException
     */
    Future<Long> append( TransactionRepresentation commands ) throws IOException;
}
