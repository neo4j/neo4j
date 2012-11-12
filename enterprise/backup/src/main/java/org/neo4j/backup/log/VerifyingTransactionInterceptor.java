/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.backup.log;

import org.neo4j.backup.check.ConsistencyCheck;
import org.neo4j.backup.check.DiffRecordStore;
import org.neo4j.backup.check.DiffStore;
import org.neo4j.backup.check.InconsistencyType;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DataInconsistencyError;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.util.StringLogger;

class VerifyingTransactionInterceptor implements TransactionInterceptor
{
    enum CheckerMode
    {
        FULL( true )
        {
            @Override
            ConsistencyCheck apply( DiffStore diffs, ConsistencyCheck checker )
            {
                try
                {
                    checker.run();
                }
                catch ( AssertionError e )
                {
                    System.err.println( e.getMessage() );
                }
                return checker;
            }
        },
        DIFF( false )
        {
            @Override
            ConsistencyCheck apply( DiffStore diffs, ConsistencyCheck checker )
            {
                return diffs.applyToAll( checker );
            }
        };
        final boolean checkProp;

        private CheckerMode( boolean checkProp )
        {
            this.checkProp = checkProp;
        }

        abstract ConsistencyCheck apply( DiffStore diffs, ConsistencyCheck checker );
    }

    private final boolean rejectInconsistentTransactions;

    private final DiffStore diffs;
    private final StringLogger msgLog;

    private LogEntry.Commit commitEntry;
    private LogEntry.Start startEntry;

    private TransactionInterceptor next;

    private final CheckerMode mode;

    VerifyingTransactionInterceptor(
            NeoStoreXaDataSource ds, CheckerMode mode, boolean rejectInconsistentTransactions )
    {
        this.rejectInconsistentTransactions = rejectInconsistentTransactions;
        this.diffs = new DiffStore( ds.getNeoStore() );
        this.msgLog = ds.getMsgLog();
        this.mode = mode;
    }

    public void setStartEntry( LogEntry.Start startEntry )
    {
        this.startEntry = startEntry;
        if ( next != null )
        {
            next.setStartEntry( startEntry );
        }
    }

    public void setCommitEntry( LogEntry.Commit commitEntry )
    {
        this.commitEntry = commitEntry;
        if (next != null)
        {
            next.setCommitEntry( commitEntry );
        }
    }

    public void setNext( TransactionInterceptor next )
    {
        this.next = next;
    }

    public void complete() throws DataInconsistencyError
    {
        /*
         *  Here goes the actual verification code. If it passes,
         *  just return - if not, throw Error so that the
         *  store remains safe.
         */
        ConsistencyCheck consistency = mode.apply( diffs, new ConsistencyCheck( diffs, mode.checkProp )
        {
            @Override
            protected <R extends AbstractBaseRecord> void report( RecordStore<R> recordStore, R record, InconsistencyType inconsistency )
            {
                if ( inconsistency.isWarning() ) return;
                StringBuilder log = messageHeader( "Inconsistencies" );
                logRecord( log, recordStore, record );
                log.append( inconsistency.message() );
                msgLog.logMessage( log.toString() );
            }

            @Override
            protected <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
                    RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
                    InconsistencyType inconsistency )
            {
                if ( inconsistency.isWarning() ) return;
                if ( recordStore == referredStore && record.getLongId() == referred.getLongId() )
                { // inconsistency between versions, logRecord() handles that, treat as single record
                    report( recordStore, record, inconsistency );
                    return;
                }
                StringBuilder log = messageHeader( "Inconsistencies" );
                logRecord( log, recordStore, record );
                logRecord( log, referredStore, referred );
                log.append( inconsistency.message() );
                msgLog.logMessage( log.toString() );
            }
        } );
        try
        {
            consistency.checkResult();
        }
        catch ( AssertionError e )
        {
            DataInconsistencyError error = new DataInconsistencyError( "Cannot apply transaction\n\t"
                                                                       + ( startEntry == null
                                                                               ? "NO START ENTRY"
                                                                               : startEntry.toString() )
                                                                       + "\n\t"
                                                                       + ( commitEntry == null
                                                                               ? "NO COMMIT ENTRY"
                                                                               : commitEntry.toString() ) + "\n\t"
                                                                       + e.getMessage() );
            msgLog.logMessage( error.getMessage() );
            if ( rejectInconsistentTransactions )
            {
                throw error;
            }
            else // we log it
            {
                // FIXME: there is a memory hazard here, this StringBuilder could grow quite large...
                final StringBuilder changes = messageHeader( "Changes" );
                diffs.applyToAll( new RecordStore.Processor()
                {
                    @Override
                    protected <R extends AbstractBaseRecord> void processRecord( Class<R> type, RecordStore<R> store,
                            R record )
                    {
                        DiffRecordStore<R> diff = (DiffRecordStore<R>) store;
                        if ( diff.isModified( record.getLongId() ) )
                        {
                            logRecord( changes, store, record );
                        }
                    }
                } );
                for ( RecordStore<?> store : diffs.allStores() )
                {
                    changes.append( store ).append( ": highId(before) = " );
                    changes.append( ( (DiffRecordStore<?>) store ).getRawHighId() );
                    changes.append( ", highId(after) = " ).append( store.getHighId() ).append( "\n\t" );
                }
                msgLog.logMessage( changes.toString() );
            }
        }
        // Chain of Responsibility continues
        if ( next != null )
        {
            next.complete();
        }
    }

    private static <R extends AbstractBaseRecord> void logRecord( StringBuilder log, RecordStore<? extends R> store,
            R record )
    {
        DiffRecordStore<? extends R> diff = (DiffRecordStore<? extends R>) store;
        if ( diff.isModified( record.getLongId() ) )
        {
            log.append( "- " ).append( diff.forceGetRaw( record.getLongId() ) ).append( "\n\t+ " );
            record = store.forceGetRecord( record.getLongId() );
        }
        log.append( record ).append( "\n\t" );
    }

    private StringBuilder messageHeader( String type )
    {
        StringBuilder log = new StringBuilder( type ).append( " in transaction" );
        if ( commitEntry != null )
            log.append( " (txId=" ).append( commitEntry.getTxId() ).append( ")" );
        else if ( startEntry != null )
            log.append( " (log local id = " ).append( startEntry.getIdentifier() ).append( ")" );
        return log.append( ":\n\t" );
    }

    @Override
    public void visitNode( NodeRecord record )
    {
        diffs.visitNode( record );
    }

    @Override
    public void visitRelationship( RelationshipRecord record )
    {
        diffs.visitRelationship( record );
    }

    @Override
    public void visitProperty( PropertyRecord record )
    {
        diffs.visitProperty( record );
    }

    @Override
    public void visitRelationshipType( RelationshipTypeRecord record )
    {
        diffs.visitRelationshipType( record );
    }

    @Override
    public void visitPropertyIndex( PropertyIndexRecord record )
    {
        diffs.visitPropertyIndex( record );
    }
}
