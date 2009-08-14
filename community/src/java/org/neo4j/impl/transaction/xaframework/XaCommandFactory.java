/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * During recovery (after system crash) the {@link XaLogicalLog} will examine
 * the the logical log and re-create any transaction that didn't complete.
 * <CODE>XaCommandFactory</CODE> job is to read in commands from the logical
 * log and re-create them.
 */
public abstract class XaCommandFactory
{
    /**
     * Reads the data from a commad previosly written to the logical log and
     * creates the command again.
     * <p>
     * The implementation has to guard against the command beeing corrupt and
     * not completly written to the logical log. When unable to read the command
     * <CODE>null</CODE> should be returned. <CODE>IOException</CODE> should
     * only be thrown in case of real IO failure. The <CODE>null</CODE> return
     * is a signal to the recovery mechanism to stop scaning for more entries.
     * 
     * @param fileChannel
     *            The channel to read the command data from
     * @param buffer
     *            A small buffer that can be used to read data into
     * @return The command or null if unable to read command
     * @throws IOException
     *             In case of real IO failure
     */
    public abstract XaCommand readCommand( ReadableByteChannel byteChannel,
        ByteBuffer buffer ) throws IOException;
}