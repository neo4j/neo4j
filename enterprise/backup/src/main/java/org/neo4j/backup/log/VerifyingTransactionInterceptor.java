/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.util.Map;

import org.neo4j.consistency.checking.old.ConsistencyRecordProcessor;
import org.neo4j.consistency.checking.old.ConsistencyReporter;
import org.neo4j.consistency.checking.old.InconsistencyType;
import org.neo4j.consistency.checking.old.MonitoringConsistencyReporter;
import org.neo4j.consistency.store.DiffRecordStore;
import org.neo4j.consistency.store.DiffStore;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DataInconsistencyError;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.StringLogger.LineLogger;

@Deprecated
class VerifyingTransactionInterceptor implements TransactionInterceptor
{
    enum CheckerMode
    {
        FULL( true )
        {
            @Override
            <R extends RecordStore.Processor<RuntimeException> & Runnable> void apply( DiffStore diffs, R checker )
            {
                try
                {
                    checker.run();
                }
                catch ( AssertionError e )
                {
                    System.err.println( e.getMessage() );
                }
            }
        },
        DIFF( false )
        {
            @Override
            <R extends RecordStore.Processor<RuntimeException> & Runnable> void apply( DiffStore diffs, R checker )
            {
                diffs.applyToAll( checker );
            }
        };
        final boolean checkProp;

        private CheckerMode( boolean checkProp )
        {
            this.checkProp = checkProp;
        }

        abstract <R extends RecordStore.Processor<RuntimeException> & Runnable> void apply( DiffStore diffs, R checker );
    }

    private final boolean rejectInconsistentTransactions;

    private final DiffStore diffs;
    private final StringLogger msgLog;

    private LogEntry.Commit commitEntry;
    private LogEntry.Start startEntry;

    private TransactionInterceptor next;

    private final CheckerMode mode;

    private final StringLogger difflog;

    VerifyingTransactionInterceptor( NeoStoreXaDataSource ds, StringLogger stringLogger, CheckerMode mode,
            boolean rejectInconsistentTransactions, Map<String, String> extraConfig )
    {
        this.rejectInconsistentTransactions = rejectInconsistentTransactions;
        this.diffs = new DiffStore( ds.getNeoStore() );
        this.msgLog = stringLogger;
        this.mode = mode;
        String log = extraConfig.get( "log" );
        this.difflog = log == null ? null : ( "true".equalsIgnoreCase( log )
                ? msgLog
                : StringLogger.logger( new File( log ) ) );
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
//        diffs, mode.checkProp
        MonitoringConsistencyReporter reporter = new MonitoringConsistencyReporter( new ConsistencyReporter()
        {
            @Override
            public <R extends AbstractBaseRecord> void report( RecordStore<R> recordStore, R record,
                                                               InconsistencyType inconsistency )
            {
                if ( inconsistency.isWarning() ) return;
                StringBuilder log = messageHeader( "Inconsistencies" ).append( "\n\t" );
                logRecord( log, recordStore, record );
                log.append( inconsistency.message() );
                msgLog.logMessage( log.toString() );
                if ( difflog != null && difflog != msgLog ) difflog.logMessage( log.toString() );
            }

            @Override
            public <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
                    RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
                    InconsistencyType inconsistency )
            {
                if ( inconsistency.isWarning() ) return;
                if ( recordStore == referredStore && record.getLongId() == referred.getLongId() )
                { // inconsistency between versions, logRecord() handles that, treat as single record
                    report( recordStore, record, inconsistency );
                    return;
                }
                StringBuilder log = messageHeader( "Inconsistencies" ).append( "\n\t" );
                logRecord( log, recordStore, record );
                logRecord( log, referredStore, referred );
                log.append( inconsistency.message() );
                msgLog.logMessage( log.toString() );
                if ( difflog != null && difflog != msgLog ) difflog.logMessage( log.toString() );
            }
        });
        mode.apply( diffs, new ConsistencyRecordProcessor( diffs, reporter ) );
        DataInconsistencyError error = null;
        try
        {
            reporter.checkResult();
        }
        catch ( AssertionError e )
        {
            error = new DataInconsistencyError( "Inconsistencies in transaction\n\t"
                                                + ( startEntry == null ? "NO START ENTRY" : startEntry.toString() )
                                                + "\n\t"
                                                + ( commitEntry == null ? "NO COMMIT ENTRY" : commitEntry.toString() )
                                                + "\n\t" + e.getMessage() );
            msgLog.logMessage( error.getMessage() );
            if ( difflog != null && difflog != msgLog ) difflog.logMessage( error.getMessage() );
        }
        if ( difflog != null || error != null )
        {
            //new DiffLogger().log( messageHeader( "Changes" ).toString() );

            final String header = messageHeader( "Changes" ).toString();
            StringLogger target = null;
            Visitor<StringLogger.LineLogger, RuntimeException> visitor = null;
            if ( error != null )
            {
                target = msgLog;
                if ( difflog != null && difflog != msgLog )
                {
                    visitor = new Visitor<StringLogger.LineLogger, RuntimeException>()
                    {
                        @Override
                        public boolean visit( final LineLogger first )
                        {
                            difflog.logLongMessage( header, new Visitor<StringLogger.LineLogger, RuntimeException>()
                            {
                                @Override
                                public boolean visit( final LineLogger other )
                                {
                                    other.logLine( startEntry == null ? "NO START ENTRY" : startEntry.toString() );
                                    other.logLine( commitEntry == null ? "NO COMMIT ENTRY" : commitEntry.toString() );
                                    logDiffLines( new LineLogger()
                                    {
                                        @Override
                                        public void logLine( String line )
                                        {
                                            first.logLine( line );
                                            other.logLine( line );
                                        }
                                    } );
                                    return false;
                                }
                            } );
                            return false;
                        }
                    };
                }
                else
                {
                    visitor = new Visitor<StringLogger.LineLogger, RuntimeException>()
                    {
                        @Override
                        public boolean visit( LineLogger lines )
                        {
                            logDiffLines( lines );
                            return false;
                        }
                    };
                }
            }
            else
            {
                target = difflog;
                visitor = new Visitor<StringLogger.LineLogger, RuntimeException>()
                {
                    @Override
                    public boolean visit( LineLogger lines )
                    {
                        lines.logLine( startEntry == null ? "NO START ENTRY" : startEntry.toString() );
                        lines.logLine( commitEntry == null ? "NO COMMIT ENTRY" : commitEntry.toString() );
                        logDiffLines( lines );
                        return false;
                    }
                };
            }
            target.logLongMessage( header, visitor );
        }
        if ( difflog != null ) difflog.close();
        // re-throw error if we are rejecting inconsistencies
        if ( error != null && rejectInconsistentTransactions ) throw error;
        // Chain of Responsibility continues
        if ( next != null ) next.complete();
    }

    private void logDiffLines( final LineLogger logger )
    {
        diffs.applyToAll( new RecordStore.Processor<RuntimeException>()
        {
            @Override
            protected <R extends AbstractBaseRecord> void processRecord( Class<R> type, RecordStore<R> store, R record )
            {
                DiffRecordStore<R> diff = (DiffRecordStore<R>) store;
                if ( diff.isModified( record.getLongId() ) )
                {
                    logRecord( logger, store, record );
                }
            }
        } );
        for ( RecordStore<?> store : diffs.allStores() )
        {
            logger.logLine( store + ": highId(before) = " + ( (DiffRecordStore<?>) store ).getRawHighId()
                            + ", highId(after) = " + store.getHighId() );
        }
    }

    private static <R extends AbstractBaseRecord> void logRecord( final StringBuilder log,
                                                                  RecordStore<? extends R> store, R record )
    {
        logRecord( new LineLogger()
        {
            @Override
            public void logLine( String line )
            {
                log.append( line ).append( "\n\t" );
            }
        }, store, record );
    }

    private static <R extends AbstractBaseRecord> void logRecord( LineLogger log, RecordStore<? extends R> store,
            R record )
    {
        DiffRecordStore<R> diff = (DiffRecordStore<R>) store;
        String prefix = "";
        if ( diff.isModified( record.getLongId() ) )
        {
            log.logLine( "- " + diff.forceGetRaw( record ) );
            prefix = "+ ";
            record = store.forceGetRecord( record.getLongId() );
        }
        log.logLine( prefix + record );
    }

    private StringBuilder messageHeader( String type )
    {
        StringBuilder log = new StringBuilder( type ).append( " in transaction" );
        if ( commitEntry != null )
            log.append( " (txId=" ).append( commitEntry.getTxId() ).append( ")" );
        else if ( startEntry != null )
            log.append( " (log local id = " ).append( startEntry.getIdentifier() ).append( ")" );
        return log.append( ':' );
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
    public void visitRelationshipTypeToken( RelationshipTypeTokenRecord record )
    {
        diffs.visitRelationshipTypeToken( record );
    }

    @Override
    public void visitLabelToken( LabelTokenRecord record )
    {
        diffs.visitLabelToken( record );
    }

    @Override
    public void visitPropertyKeyToken( PropertyKeyTokenRecord record )
    {
        diffs.visitPropertyKeyToken( record );
    }

    @Override
    public void visitNeoStore( NeoStoreRecord record )
    {
        diffs.visitNeoStore( record );
    }
}
