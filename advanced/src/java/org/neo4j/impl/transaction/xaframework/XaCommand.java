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

// TODO: make writeToFile safe (not being able to do anything but write a
// sequence of bytes that contains the command)

/**
 * A command is part of the work done during a transaction towards a XA
 * resource. Any modifying operation invoked on a <CODE>XaResource</CODE> must
 * be in a transaction and wrapped in a <CODE>XaCommand</CODE>. Each command
 * created will be written to the {@link XaLogicalLog} once it is added to the
 * transaction.
 * <p>
 * Commands are memory buckets containing information on how to presist the
 * operation in the XA resource. Any nececcery constraint checks must take place
 * before any command has been executed. Once a command in the transaction has
 * started to execute there is no turning back, all of the following commands
 * must execute without problems. To ensure this check the constraints via a tx
 * synchronization hook, at prepare or upon command creation and signal the
 * transaction before it starts to commit if something is wrong.
 * <p>
 * Throwing exception when the command rollbacks or executes could (depending on
 * TransactionManager implementation) bring down the whole system. Should
 * however a <CODE>execute</CODE> or <CODE>rollback</CODE> call fail throw a
 * proper runtime exception.
 * <p>
 * Note that a command is during normal execution only executed once but it must
 * be able to execute again "re-writing" it self to underlying persistance store
 * during recovery. Think of the following scenario:
 * <p>
 * A transaction containing 50 commands begins to commit. When 24.5 commands
 * have been executed the system crashes. System is brought up again and the
 * transaction is recreated. The transaction manager tells the resource to
 * commit (again) and the 25 first commands will be executed for a second time
 * while the 25 last commands will be executed for the first time. Also it would
 * be possible for the system to crash during recovery causing commands to
 * execute again and again and so on.
 */
public abstract class XaCommand
{
    private boolean isRecovered = false;

    /**
     * Default implementation of rollback that does nothing. This method is not
     * to undo any work done by the {@link #execute} method. Commands in a
     * {@link XaTransaction} are either all rolled back or all executed, they're
     * not linked together as usual execute/rollback methods.
     * <p>
     * Since a command only is in memory nothing has been made persistent so
     * rollback usually don't have to do anything. Sometimes however a command
     * needs to acquire resources when created (since the application thinks it
     * has done the work when the command is created). For example, if a command
     * creates some entity that has a primary id we need to generate that id
     * upon command creation. But if the command is rolled back we should
     * release that id. This is the place to do just that.
     */
    public void rollback()
    {
    };

    /**
     * Executes the command and makes it persistent. This method must succeed,
     * any protests about this command not being able to execute should be done
     * before execution of any command within the transaction.
     */
    public abstract void execute();

    /**
     * When a command is added to a transaction (usually when it is created) it
     * must be written to the {@link XaLogicalLog}. This method should write
     * all the data that is needed to re-create the command (see
     * {@link XaCommandFactory}).
     * <p>
     * Write the data to the <CODE>fileChannel</CODE>, you can use the 
     * <CODE>buffer</CODE> supplied or create your own buffer since its capacity
     * is very small (137 bytes or something). Acccess to writing commands is 
     * synchronized, only one command will be written at a time so if you need 
     * to write larger data sets the commands can share the same buffer.
     * <p>
     * Don't throw an <CODE>IOException</CODE> to imply something is wrong
     * with the command. An exception should only be thrown here if there is a
     * real IO failure. If something is wrong with this command it should have
     * been detected when it was created.
     * <p>
     * Don't <CODE>force</CODE>, <CODE>position</CODE> or anything except
     * normal forward <CODE>write</CODE> with the file channel.
     * 
     * @param fileChannel
     *            The channel to the {@link XaLogicalLog}
     * @param buffer
     *            A small byte buffer that can be used to write command data
     * @throws IOException
     *             In case of *real* IO failure
     */
    public abstract void writeToFile( LogBuffer buffer ) throws IOException;

    /**
     * If this command is created by the command factory during a recovery scan
     * of the logical log this method will be called to mark the command as a
     * "recovered command".
     */
    protected void setRecovered()
    {
        isRecovered = true;
    }

    /**
     * Returns wether or not this is a "recovered command".
     * 
     * @return <CODE>true</CODE> if command was created during a recovery else
     *         <CODE>false</CODE> is returned
     */
    public boolean isRecovered()
    {
        return isRecovered;
    }
}