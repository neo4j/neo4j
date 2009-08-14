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
import javax.transaction.xa.XAException;

/**
 * <CODE>XaTransaction</CODE> holds all the commands that participate in the
 * transaction and then either rollbacks or commits them. Here are two example
 * implementations:
 * <p>
 * 
 * <pre>
 * <CODE>
 * // Example of XaTransaction implementation where commands are written to
 * // to logical log directly when created
 * public class MyTransaction extends XaTransaction
 * {
 *     private List cmds = new java.util.LinkedList();
 * 
 *     public boolean isReadyOnly()
 *     {
 *         return cmds.size() == 0;
 *     }
 * 
 *     public void doAddCommand( XaCommand cmd )
 *     {
 *         cmds.add( cmd );
 *     }
 * 
 *     public void doRollback()
 *     {
 *         Iterator itr = cmds.iterator();
 *         while ( itr.hasNext() )
 *             ((XaCommand) itr.next()).rollback();
 *     }
 * 
 *     public void doCommit()
 *     {
 *         Iterator itr = cmds.iterator();
 *         while ( itr.hasNext() )
 *             ((XaCommand) itr.next()).execute();
 *     }
 * 
 *     public void doPrepare()
 *     {
 *         // do nothing since commands are added before prepare
 *     }
 * }
 * <CODE>
 * </pre>
 * 
 * Some other implementation that makes use of prepare could look something like
 * this:
 * 
 * <pre>
 * <CODE>
 * // Example of XaTransaction implementation where commands are written to
 * // to logical log when transaction is prepared
 * public class MyTransaction extends XaTransaction
 * {
 *     private List cmds = new java.util.LinkedList();
 * 
 *     public boolean isReadyOnly()
 *     {
 *         return cmds.size() == 0;
 *     }
 * 
 *     public void doAddCommand( XaCommand cmd )
 *     {
 *         // do nothing, we call addCommand in prepare 
 *     }
 * 
 *     public void doRollback()
 *     {
 *         Iterator itr = cmds.iterator();
 *         while ( itr.hasNext() )
 *             ((XaCommand) itr.next()).rollback();
 *     }
 * 
 *     public void doCommit()
 *     {
 *         Iterator itr = cmds.iterator();
 *         while ( itr.hasNext() )
 *             ((XaCommand) itr.next()).execute();
 *     }
 * 
 *     public void doPrepare()
 *     {
 *         Iterator itr = cmds.iterator();
 *         while ( itr.hasNext() )
 *             addCommand( (XaCommand) itr.next() );
 *     }
 * 
 * }
 * </CODE>
 * </pre>
 */
public abstract class XaTransaction
{
    /**
     * Returns <CODE>true</CODE> if read only transaction, that is no
     * modifications will be made once the transaction commits.
     * 
     * @return true if read only transaction
     */
    public abstract boolean isReadOnly();

    /**
     * When a command is added to transaction it will be passed via this method.
     * The <CODE>XaTransaction</CODE> needs to hold all the commands in memory
     * until it receives the <CODE>doCommit</CODE> or <CODE>doRollback</CODE>
     * call.
     * 
     * @param command
     *            The command to be added to transaction
     */
    protected abstract void doAddCommand( XaCommand command );

    /**
     * Rollbacks the transaction, loop through all commands and invoke <CODE>rollback()</CODE>.
     * 
     * @throws XAException
     *             If unable to rollback
     */
    protected abstract void doRollback() throws XAException;

    /**
     * Called when transaction is beeing prepared.
     * 
     * @throws XAException
     *             If unable to prepare
     */
    protected abstract void doPrepare() throws XAException;

    /**
     * Commits the transaction, loop through all commands and invoke 
     * <CODE>execute()</CODE>.
     * 
     * @throws XAEXception
     *             If unable to commit
     */
    protected abstract void doCommit() throws XAException;

    private final int identifier;
    private final XaLogicalLog log;
    private boolean isRecovered = false;
    private boolean committed = false;
    private boolean rolledback = false;
    private boolean prepared = false;

    public XaTransaction( int identifier, XaLogicalLog log )
    {
        if ( log == null )
        {
            throw new IllegalArgumentException( "LogicalLog is null" );
        }
        this.identifier = identifier;
        this.log = log;
    }

    /**
     * If this transacgtion is created during a recovery scan of the logical log
     * method will be called to mark the transaction "recovered".
     */
    protected void setRecovered()
    {
        isRecovered = true;
    }

    /**
     * Returns <CODE>true</CODE> if this is a "recovered transaction".
     * 
     * @return <CODE>true</CODE> if transaction was created during a recovery
     *         else <CODE>false</CODE> is returned
     */
    public boolean isRecovered()
    {
        return isRecovered;
    }

    /**
     * Returns the "internal" identifier for this transaction. See
     * {@link XaLogicalLog#getCurrentTxIdentifier}.
     * 
     * @return The transaction identifier
     */
    public final int getIdentifier()
    {
        return identifier;
    }

    /**
     * Adds the command to transaction. First writes the command to the logical
     * log then calls {@link #doAddCommand}. Also check
     * {@link XaConnectionHelpImpl} class documentation example.
     * 
     * @param command
     *            The command to add to transaction
     * @throws RuntimeException
     *             If problem writing command to logical log or this transaction
     *             is committed or rolled back
     */
    public final void addCommand( XaCommand command )
    {
        if ( committed )
        {
            throw new RuntimeException(
                "Cannot add command to committed transaction" );
        }
        if ( rolledback )
        {
            throw new RuntimeException(
                "Cannot add command to rolled back transaction" );
        }
        doAddCommand( command );
        try
        {
            log.writeCommand( command, identifier );
        }
        catch ( IOException e )
        {
            throw new RuntimeException(
                "Unable to write command to logical log.", e );
        }
    }

    /**
     * Used during recovery, calls {@link #doAddCommand}. Injects the command
     * into the transaction without writing to the logical log.
     * 
     * @param command
     *            The command that will be injected
     */
    protected void injectCommand( XaCommand command )
    {
        doAddCommand( command );
    }

    /**
     * Rollbacks the transaction, calls {@link #doRollback}.
     * 
     * @throws XAException
     *             If unable to rollback
     */
    public final void rollback() throws XAException
    {
        if ( committed )
        {
            throw new XAException(
                "Cannot rollback partialy commited transaction. Recover and "
                    + "commit" );
        }
        rolledback = true;
        doRollback();
    }

    /**
     * Called before prepare marker is written to logical log. Calls
     * {@link #doPrepare()}.
     * 
     * @throws XAException
     *             if unable to prepare
     */
    public final void prepare() throws XAException
    {
        if ( committed )
        {
            throw new XAException( "Cannot prepare comitted transaction" );
        }
        if ( rolledback )
        {
            throw new XAException( "Cannot prepare rolled back transaction" );
        }
        prepared = true;
        doPrepare();
    }

    /**
     * First registers the transaction identifier (see
     * {@link XaLogicalLog#getCurrentTxIdentifier} then calls {@link #doCommit}.
     * 
     * @throws XAException
     *             If unable to commit
     */
    public final void commit() throws XAException
    {
        if ( !prepared && !isRecovered() )
        {
            throw new XAException( "Cannot commit unprepared transaction" );
        }
        log.registerTxIdentifier( getIdentifier() );
        try
        {
            committed = true;
            doCommit();
        }
        finally
        {
            log.unregisterTxIdentifier();
        }
    }
}