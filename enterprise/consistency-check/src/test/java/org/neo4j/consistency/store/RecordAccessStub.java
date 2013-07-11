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
package org.neo4j.consistency.store;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.PendingReferenceCheck;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;

import static java.util.Collections.singletonMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class RecordAccessStub implements RecordAccess, DiffRecordAccess
{

    public static final int SCHEMA_RECORD_TYPE = 255;

    @SuppressWarnings("unchecked")
    public <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    REPORT mockReport( Class<REPORT> reportClass, RECORD record )
    {
        REPORT report = mock( reportClass );
        doAnswer( new DeferredReferenceDispatch( report, record ) )
                .when( report ).forReference( any( RecordReference.class ), any( ComparativeRecordChecker.class ) );
        return report;
    }

    @SuppressWarnings("unchecked")
    public <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    REPORT mockReport( Class<REPORT> reportClass, RECORD oldRecord, RECORD newRecord )
    {
        REPORT report = mock( reportClass );
        doAnswer( new DeferredReferenceDispatch( report, oldRecord, newRecord ) )
                .when( report ).forReference( any( RecordReference.class ), any( ComparativeRecordChecker.class ) );
        return report;
    }

    private class DeferredReferenceDispatch<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
            implements Answer<Void>
    {
        private final REPORT report;
        private final RECORD oldRecord;
        private final RECORD newRecord;

        DeferredReferenceDispatch( REPORT report, RECORD oldRecord, RECORD newRecord )
        {
            this.report = report;
            this.oldRecord = oldRecord;
            this.newRecord = newRecord;
        }

        DeferredReferenceDispatch( REPORT report, RECORD record )
        {
            this.report = report;
            this.oldRecord = null;
            this.newRecord = record;
        }

        @Override
        public Void answer( InvocationOnMock invocation ) throws Throwable
        {
            Object[] arguments = invocation.getArguments();
            forReference( (RecordReference) arguments[0], (ComparativeRecordChecker) arguments[1] );
            return null;
        }

        private void forReference( final RecordReference reference, final ComparativeRecordChecker checker )
        {
            deferredTasks.add( new Runnable()
            {
                @Override
                @SuppressWarnings("unchecked")
                public void run()
                {
                    PendingReferenceCheck mock = mock( PendingReferenceCheck.class );
                    DeferredReferenceCheck check = new DeferredReferenceCheck( DeferredReferenceDispatch.this,
                                                                               checker );
                    doAnswer( check ).when( mock ).checkReference( any( AbstractBaseRecord.class ),
                                                                   any( RecordAccess.class ) );
                    doAnswer( check ).when( mock ).checkDiffReference( any( AbstractBaseRecord.class ),
                                                                       any( AbstractBaseRecord.class ),
                                                                       any( RecordAccess.class ) );
                    reference.dispatch( mock );
                }
            } );
        }

        void checkReference( final ComparativeRecordChecker checker, final AbstractBaseRecord oldReference, final AbstractBaseRecord newReference )
        {
            deferredTasks.add( new Runnable()
            {
                @Override
                @SuppressWarnings("unchecked")
                public void run()
                {
                    checker.checkReference( newRecord, newReference, report, RecordAccessStub.this );
                }
            } );
        }
    }

    private static class DeferredReferenceCheck implements Answer<Void>
    {
        private final DeferredReferenceDispatch dispatch;
        private final ComparativeRecordChecker checker;

        DeferredReferenceCheck( DeferredReferenceDispatch dispatch, ComparativeRecordChecker checker )
        {
            this.dispatch = dispatch;
            this.checker = checker;
        }

        @Override
        public Void answer( InvocationOnMock invocation ) throws Throwable
        {
            Object[] arguments = invocation.getArguments();
            AbstractBaseRecord oldReference = null, newReference;
            if ( arguments.length == 3 )
            {
                oldReference = (AbstractBaseRecord) arguments[0];
                newReference = (AbstractBaseRecord) arguments[1];
            }
            else
            {
                newReference = (AbstractBaseRecord) arguments[0];
            }
            dispatch.checkReference( checker, oldReference, newReference );
            return null;
        }
    }

    private final Queue<Runnable> deferredTasks = new LinkedList<Runnable>();

    public void checkDeferred()
    {
        for ( Runnable task; null != (task = deferredTasks.poll()); )
        {
            task.run();
        }
    }

    private final Map<Long, Delta<DynamicRecord>> schemata = new HashMap<Long, Delta<DynamicRecord>>();
    private final Map<Long, Delta<NodeRecord>> nodes = new HashMap<Long, Delta<NodeRecord>>();
    private final Map<Long, Delta<RelationshipRecord>> relationships = new HashMap<Long, Delta<RelationshipRecord>>();
    private final Map<Long, Delta<PropertyRecord>> properties = new HashMap<Long, Delta<PropertyRecord>>();
    private final Map<Long, Delta<DynamicRecord>> strings = new HashMap<Long, Delta<DynamicRecord>>();
    private final Map<Long, Delta<DynamicRecord>> arrays = new HashMap<Long, Delta<DynamicRecord>>();
    private final Map<Long, Delta<RelationshipTypeTokenRecord>> relationshipTypeTokens = new HashMap<Long, Delta<RelationshipTypeTokenRecord>>();
    private final Map<Long, Delta<LabelTokenRecord>> labelTokens = new HashMap<Long, Delta<LabelTokenRecord>>();
    private final Map<Long, Delta<PropertyKeyTokenRecord>> propertyKeyTokens = new HashMap<Long, Delta<PropertyKeyTokenRecord>>();
    private final Map<Long, Delta<DynamicRecord>> relationshipTypeNames = new HashMap<Long, Delta<DynamicRecord>>();
    private final Map<Long, Delta<DynamicRecord>> nodeDynamicLabels = new HashMap<Long, Delta<DynamicRecord>>();
    private final Map<Long, Delta<DynamicRecord>> labelNames = new HashMap<Long, Delta<DynamicRecord>>();
    private final Map<Long, Delta<DynamicRecord>> propertyKeyNames = new HashMap<Long, Delta<DynamicRecord>>();
    private Delta<NeoStoreRecord> graph;

    private static class Delta<R extends AbstractBaseRecord>
    {
        final R oldRecord, newRecord;

        Delta( R record )
        {
            this.oldRecord = null;
            this.newRecord = record;
        }

        Delta( R oldRecord, R newRecord )
        {
            this.oldRecord = oldRecord;
            this.newRecord = newRecord;
        }
    }

    private enum Version
    {
        PREV
        {
            @Override
            <R extends AbstractBaseRecord> R get( Delta<R> delta )
            {
                return delta.oldRecord == null ? delta.newRecord : delta.oldRecord;
            }
        },
        LATEST
        {
            @Override
            <R extends AbstractBaseRecord> R get( Delta<R> delta )
            {
                return delta.newRecord;
            }
        },
        NEW
        {
            @Override
            <R extends AbstractBaseRecord> R get( Delta<R> delta )
            {
                return delta.oldRecord == null ? null : delta.newRecord;
            }
        };

        abstract <R extends AbstractBaseRecord> R get( Delta<R> delta );
    }

    private static <R extends AbstractBaseRecord> R add( Map<Long, Delta<R>> records, R record )
    {
        records.put( record.getLongId(), new Delta<R>( record ) );
        return record;
    }

    private static <R extends AbstractBaseRecord> void add( Map<Long, Delta<R>> records, R oldRecord, R newRecord )
    {
        records.put( newRecord.getLongId(), new Delta<R>( oldRecord, newRecord ) );
    }

    public DynamicRecord addSchema( DynamicRecord schema )
    {
        return add( schemata, schema);
    }

    public DynamicRecord addString( DynamicRecord string )
    {
        return add( strings, string );
    }

    public DynamicRecord addArray( DynamicRecord array )
    {
        return add( arrays, array );
    }

    public DynamicRecord addNodeDynamicLabels( DynamicRecord array )
    {
        return add( nodeDynamicLabels, array );
    }

    public DynamicRecord addPropertyKeyName( DynamicRecord name )
    {
        return add( propertyKeyNames, name );
    }

    public DynamicRecord addRelationshipTypeName( DynamicRecord name )
    {
        return add( relationshipTypeNames, name );
    }

    public DynamicRecord addLabelName( DynamicRecord name )
    {
        return add( labelNames, name );
    }

    public <R extends AbstractBaseRecord> R addChange( R oldRecord, R newRecord )
    {
        if ( newRecord instanceof NodeRecord )
        {
            add( nodes, (NodeRecord) oldRecord, (NodeRecord) newRecord );
        }
        else if ( newRecord instanceof RelationshipRecord )
        {
            add( relationships, (RelationshipRecord) oldRecord, (RelationshipRecord) newRecord );
        }
        else if ( newRecord instanceof PropertyRecord )
        {
            add( properties, (PropertyRecord) oldRecord, (PropertyRecord) newRecord );
        }
        else if ( newRecord instanceof DynamicRecord )
        {
            DynamicRecord dyn = (DynamicRecord) newRecord;
            if ( dyn.getType() == PropertyType.STRING.intValue() )
            {
                add( strings, (DynamicRecord) oldRecord, dyn );
            }
            else if ( dyn.getType() == PropertyType.ARRAY.intValue() )
            {
                add( arrays, (DynamicRecord) oldRecord, dyn );
            }
            else if ( dyn.getType() == SCHEMA_RECORD_TYPE )
            {
                add( schemata, (DynamicRecord) oldRecord, dyn );
            }
            else
            {
                throw new IllegalArgumentException( "Invalid dynamic record type" );
            }
        }
        else if ( newRecord instanceof RelationshipTypeTokenRecord )
        {
            add( relationshipTypeTokens, (RelationshipTypeTokenRecord) oldRecord, (RelationshipTypeTokenRecord) newRecord );
        }
        else if ( newRecord instanceof PropertyKeyTokenRecord )
        {
            add( propertyKeyTokens, (PropertyKeyTokenRecord) oldRecord, (PropertyKeyTokenRecord) newRecord );
        }
        else if ( newRecord instanceof NeoStoreRecord )
        {
            this.graph = new Delta<NeoStoreRecord>( (NeoStoreRecord) oldRecord, (NeoStoreRecord) newRecord );
        }
        else
        {
            throw new IllegalArgumentException( "Invalid record type" );
        }
        return newRecord;
    }

    public <R extends AbstractBaseRecord> R add( R record )
    {
        if ( record instanceof NodeRecord )
        {
            add( nodes, (NodeRecord) record );
        }
        else if ( record instanceof RelationshipRecord )
        {
            add( relationships, (RelationshipRecord) record );
        }
        else if ( record instanceof PropertyRecord )
        {
            add( properties, (PropertyRecord) record );
        }
        else if ( record instanceof DynamicRecord )
        {
            DynamicRecord dyn = (DynamicRecord) record;
            if ( dyn.getType() == PropertyType.STRING.intValue() )
            {
                addString( dyn );
            }
            else if ( dyn.getType() == PropertyType.ARRAY.intValue() )
            {
                addArray( dyn );
            }
            else if ( dyn.getType() == SCHEMA_RECORD_TYPE )
            {
                addSchema( dyn );
            }
            else
            {
                throw new IllegalArgumentException( "Invalid dynamic record type" );
            }
        }
        else if ( record instanceof RelationshipTypeTokenRecord )
        {
            add( relationshipTypeTokens, (RelationshipTypeTokenRecord) record );
        }
        else if ( record instanceof PropertyKeyTokenRecord )
        {
            add( propertyKeyTokens, (PropertyKeyTokenRecord) record );
        }
        else if ( record instanceof LabelTokenRecord )
        {
            add( labelTokens, (LabelTokenRecord) record );
        }
        else if ( record instanceof NeoStoreRecord )
        {
            this.graph = new Delta<NeoStoreRecord>( (NeoStoreRecord) record );
        }
        else
        {
            throw new IllegalArgumentException( "Invalid record type" );
        }
        return record;
    }

    private <R extends AbstractBaseRecord> DirectRecordReference<R> reference( Map<Long, Delta<R>> records,
                                                                               long id, Version version )
    {
        return new DirectRecordReference<R>( record( records, id, version ), this );
    }

    private static <R extends AbstractBaseRecord> R record( Map<Long, Delta<R>> records, long id,
                                                            Version version )
    {
        Delta<R> delta = records.get( id );
        if ( delta == null )
        {
            if ( version == Version.NEW )
            {
                return null;
            }
            throw new AssertionError( String.format( "Access to record with id=%d not expected.", id ) );
        }
        return version.get( delta );
    }

    @Override
    public RecordReference<DynamicRecord> schema( long id )
    {
        return reference( schemata, id, Version.LATEST );
    }

    @Override
    public RecordReference<NodeRecord> node( long id )
    {
        return reference( nodes, id, Version.LATEST );
    }

    @Override
    public RecordReference<RelationshipRecord> relationship( long id )
    {
        return reference( relationships, id, Version.LATEST );
    }

    @Override
    public RecordReference<PropertyRecord> property( long id )
    {
        return reference( properties, id, Version.LATEST );
    }

    @Override
    public RecordReference<RelationshipTypeTokenRecord> relationshipType( int id )
    {
        return reference( relationshipTypeTokens, id, Version.LATEST );
    }

    @Override
    public RecordReference<PropertyKeyTokenRecord> propertyKey( int id )
    {
        return reference( propertyKeyTokens, id, Version.LATEST );
    }

    @Override
    public RecordReference<DynamicRecord> string( long id )
    {
        return reference( strings, id, Version.LATEST );
    }

    @Override
    public RecordReference<DynamicRecord> array( long id )
    {
        return reference( arrays, id, Version.LATEST );
    }

    @Override
    public RecordReference<DynamicRecord> relationshipTypeName( int id )
    {
        return reference( relationshipTypeNames, id, Version.LATEST );
    }

    @Override
    public RecordReference<DynamicRecord> nodeLabels( long id )
    {
        return reference( nodeDynamicLabels, id, Version.LATEST );
    }

    @Override
    public RecordReference<LabelTokenRecord> label( int id )
    {
        return reference( labelTokens, id, Version.LATEST );
    }

    @Override
    public RecordReference<DynamicRecord> labelName( int id )
    {
        return reference( labelNames, id, Version.LATEST );
    }

    @Override
    public RecordReference<DynamicRecord> propertyKeyName( int id )
    {
        return reference( propertyKeyNames, id, Version.LATEST );
    }

    @Override
    public RecordReference<NeoStoreRecord> graph()
    {
        return reference( singletonMap( -1L, graph ), -1, Version.LATEST );
    }

    @Override
    public RecordReference<NodeRecord> previousNode( long id )
    {
        return reference( nodes, id, Version.PREV );
    }

    @Override
    public RecordReference<RelationshipRecord> previousRelationship( long id )
    {
        return reference( relationships, id, Version.PREV );
    }

    @Override
    public RecordReference<PropertyRecord> previousProperty( long id )
    {
        return reference( properties, id, Version.PREV );
    }

    @Override
    public DynamicRecord changedSchema( long id )
    {
        return record( schemata, id, Version.NEW );
    }

    @Override
    public NodeRecord changedNode( long id )
    {
        return record( nodes, id, Version.NEW );
    }

    @Override
    public RelationshipRecord changedRelationship( long id )
    {
        return record( relationships, id, Version.NEW );
    }

    @Override
    public PropertyRecord changedProperty( long id )
    {
        return record( properties, id, Version.NEW );
    }

    @Override
    public DynamicRecord changedString( long id )
    {
        return record( strings, id, Version.NEW );
    }

    @Override
    public DynamicRecord changedArray( long id )
    {
        return record( arrays, id, Version.NEW );
    }

    @Override
    public RecordReference<NeoStoreRecord> previousGraph()
    {
        return reference( singletonMap( -1L, graph ), -1, Version.PREV );
    }
}
