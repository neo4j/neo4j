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
package org.neo4j.snapshot;

import java.io.PrintStream;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.neo4j.common.DependencyResolver;
import org.neo4j.cypher.internal.javacompat.SnapshotExecutionEngine;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.Predicates;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.kernel.impl.context.TransactionVersionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * A {@link VersionContext} that can be injected in tests to verify the behavior of {@link SnapshotExecutionEngine}.
 */
public class TestVersionContext extends TransactionVersionContext
{
    private boolean wrongLastClosedTxId = true;
    private int numIsDirtyCalls;
    private int additionalAttempts;
    private boolean stayDirty;
    private Exception lastMarkAsDirtyCall;
    private Exception additionalAttemptsCall;
    private volatile Predicate<Thread> threadFilter = Predicates.alwaysTrue();

    public TestVersionContext( LongSupplier transactionIdSupplier )
    {
        super( transactionIdSupplier );
    }

    @Override
    public long lastClosedTransactionId()
    {
        return wrongLastClosedTxId ? TransactionIdStore.BASE_TX_ID : super.lastClosedTransactionId();
    }

    @Override
    public void markAsDirty()
    {
        if ( !onCorrectThread() )
        {
            // From some other background thread, ignore this
            return;
        }

        super.markAsDirty();
        if ( !stayDirty )
        {
            wrongLastClosedTxId = false;
        }
        lastMarkAsDirtyCall = new Exception( "markAsDirty" );
    }

    protected boolean onCorrectThread()
    {
        return threadFilter.test( Thread.currentThread() );
    }

    @Override
    public boolean isDirty()
    {
        numIsDirtyCalls++;
        boolean dirty = super.isDirty();
        if ( dirty )
        {
            additionalAttempts++;
            additionalAttemptsCall = new Exception( "isDirty" );
        }
        return dirty;
    }

    public void printDirtyCalls( PrintStream printStream )
    {
        if ( lastMarkAsDirtyCall != null )
        {
            lastMarkAsDirtyCall.printStackTrace( printStream );
        }
        else
        {
            printStream.println( "No last markAsDirty call" );
        }

        if ( additionalAttemptsCall != null )
        {
            additionalAttemptsCall.printStackTrace( printStream );
        }
        else
        {
            printStream.println( "No additionalAttempts call" );
        }
    }

    public int getNumIsDirtyCalls()
    {
        return numIsDirtyCalls;
    }

    public int getAdditionalAttempts()
    {
        return additionalAttempts;
    }

    public void setWrongLastClosedTxId( boolean wrongLastClosedTxId )
    {
        this.wrongLastClosedTxId = wrongLastClosedTxId;
    }

    public void stayDirty( boolean stayDirty )
    {
        this.stayDirty = stayDirty;
    }

    public void onlyCareAboutCurrentThread()
    {
        Thread threadToFilterOn = Thread.currentThread();
        setThreadFilter( t -> t.equals( threadToFilterOn ) );
    }

    public void setThreadFilter( Predicate<Thread> filter )
    {
        this.threadFilter = filter;
    }

    public static TestVersionContext testCursorContext( LongSupplier idSupplier )
    {
        return new TestVersionContext( idSupplier );
    }

    public static TestVersionContext testCursorContext( DatabaseManagementService managementService,  String databaseName )
    {
        TransactionIdStore transactionIdStore = getTransactionIdStore( managementService, databaseName );
        return new TestVersionContext( transactionIdStore::getLastClosedTransactionId );
    }

    private static TransactionIdStore getTransactionIdStore( DatabaseManagementService managementService, String databaseName )
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) managementService.database( databaseName )).getDependencyResolver();
        return dependencyResolver.resolveDependency( TransactionIdStore.class );
    }
}
