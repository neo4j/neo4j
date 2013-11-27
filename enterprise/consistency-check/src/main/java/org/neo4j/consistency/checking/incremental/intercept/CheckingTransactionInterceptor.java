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
package org.neo4j.consistency.checking.incremental.intercept;

import java.util.Collection;

import org.neo4j.consistency.ConsistencyCheckingError;
import org.neo4j.consistency.checking.InconsistentStoreException;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.incremental.DiffCheck;
import org.neo4j.consistency.store.DiffStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;

class CheckingTransactionInterceptor implements TransactionInterceptor
{
    private TransactionInterceptor next;
    private LogEntry.Start startEntry;
    private LogEntry.Commit commitEntry;
    private final DiffStore diffs;
    private final DiffCheck checker;

    CheckingTransactionInterceptor( DiffCheck checker, NeoStoreXaDataSource dataSource )
    {
        this.checker = checker;
        this.diffs = new DiffStore( dataSource.getNeoStore() );
    }

    void setNext( TransactionInterceptor next )
    {
        this.next = next;
    }

    @Override
    public void setStartEntry( LogEntry.Start startEntry )
    {
        this.startEntry = startEntry;
        if ( next != null )
        {
            next.setStartEntry( startEntry );
        }
    }

    @Override
    public void setCommitEntry( LogEntry.Commit commitEntry )
    {
        this.commitEntry = commitEntry;
        if ( next != null )
        {
            next.setCommitEntry( commitEntry );
        }
    }

    @Override
    public void visitNode( NodeRecord record )
    {
        diffs.visitNode( record );
        if ( next != null )
        {
            next.visitNode( record );
        }
    }

    @Override
    public void visitRelationship( RelationshipRecord record )
    {
        diffs.visitRelationship( record );
        if ( next != null )
        {
            next.visitRelationship( record );
        }
    }

    @Override
    public void visitProperty( PropertyRecord record )
    {
        diffs.visitProperty( record );
        if ( next != null )
        {
            next.visitProperty( record );
        }
    }

    @Override
    public void visitRelationshipTypeToken( RelationshipTypeTokenRecord record )
    {
        diffs.visitRelationshipTypeToken( record );
        if ( next != null )
        {
            next.visitRelationshipTypeToken( record );
        }
    }

    @Override
    public void visitLabelToken( LabelTokenRecord record )
    {
        diffs.visitLabelToken( record );
        if ( next != null )
        {
            next.visitLabelToken( record );
        }
    }

    @Override
    public void visitPropertyKeyToken( PropertyKeyTokenRecord record )
    {
        diffs.visitPropertyKeyToken( record );
        if ( next != null )
        {
            next.visitPropertyKeyToken( record );
        }
    }

    @Override
    public void visitNeoStore( NeoStoreRecord record )
    {
        diffs.visitNeoStore( record );
        if ( next != null )
        {
            next.visitNeoStore( record );
        }
    }

    @Override
    public void visitSchemaRule( Collection<DynamicRecord> records )
    {
        diffs.visitSchemaRule( records );
        if ( next != null )
        {
            next.visitSchemaRule( records );
        }
    }

    @Override
    public void complete() throws ConsistencyCheckingError
    {
        // TODO: move the logging code from VerifyingTransactionInterceptor to this class, then remove that class
        try
        {
            checker.check( diffs );
        }
        catch ( InconsistentStoreException inconsistency )
        {
            throw new ConsistencyCheckingError( startEntry, commitEntry, inconsistency.summary() );
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            // TODO: is this sufficient handling? -- probably add it to messages.log
            e.printStackTrace();
        }
    }
}
