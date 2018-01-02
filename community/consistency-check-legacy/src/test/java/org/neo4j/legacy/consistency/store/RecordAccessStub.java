/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.legacy.consistency.store;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.legacy.consistency.checking.CheckerEngine;
import org.neo4j.legacy.consistency.checking.ComparativeRecordChecker;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.report.PendingReferenceCheck;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.DirectRecordReference;
import org.neo4j.legacy.consistency.store.RecordAccess;
import org.neo4j.legacy.consistency.store.RecordReference;

import static java.util.Collections.singletonMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class RecordAccessStub implements RecordAccess, DiffRecordAccess
{
    public static final int SCHEMA_RECORD_TYPE = 255;

    public <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport>
    CheckerEngine<RECORD, REPORT> engine( final RECORD record, final REPORT report )
    {
        return new Engine<RECORD, REPORT>( report )
        {
            @Override
            @SuppressWarnings("unchecked")
            void checkReference( ComparativeRecordChecker checker, AbstractBaseRecord oldReference,
                                 AbstractBaseRecord newReference )
            {
                checker.checkReference( record, newReference, this, RecordAccessStub.this );
            }
        };
    }

    public <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport>
    CheckerEngine<RECORD, REPORT> engine( final RECORD oldRecord, final RECORD newRecord, REPORT report )
    {
        return new Engine<RECORD, REPORT>( report )
        {
            @Override
            @SuppressWarnings("unchecked")
            void checkReference( ComparativeRecordChecker checker, AbstractBaseRecord oldReference,
                                 AbstractBaseRecord newReference )
            {
                checker.checkReference( newRecord, newReference, this, RecordAccessStub.this );
            }
        };
    }

    private abstract class Engine<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport>
            implements CheckerEngine<RECORD, REPORT>
    {
        private final REPORT report;

        protected Engine( REPORT report )
        {
            this.report = report;
        }

        @Override
        public <REFERRED extends AbstractBaseRecord> void comparativeCheck(
                final RecordReference<REFERRED> other,
                final ComparativeRecordChecker<RECORD, ? super REFERRED, REPORT> checker )
        {
            deferredTasks.add( new Runnable()
            {
                @Override
                @SuppressWarnings("unchecked")
                public void run()
                {
                    PendingReferenceCheck mock = mock( PendingReferenceCheck.class );
                    DeferredReferenceCheck check = new DeferredReferenceCheck( Engine.this, checker );
                    doAnswer( check ).when( mock ).checkReference( any( AbstractBaseRecord.class ),
                                                                   any( RecordAccess.class ) );
                    doAnswer( check ).when( mock ).checkDiffReference( any( AbstractBaseRecord.class ),
                                                                       any( AbstractBaseRecord.class ),
                                                                       any( RecordAccess.class ) );
                    other.dispatch( mock );
                }
            } );
        }

        @Override
        public REPORT report()
        {
            return report;
        }

        abstract void checkReference( ComparativeRecordChecker checker, AbstractBaseRecord oldReference, AbstractBaseRecord newReference );
    }

    private static class DeferredReferenceCheck implements Answer<Void>
    {
        private final Engine dispatch;
        private final ComparativeRecordChecker checker;

        DeferredReferenceCheck( Engine dispatch, ComparativeRecordChecker checker )
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

    private final Queue<Runnable> deferredTasks = new LinkedList<>();

    public void checkDeferred()
    {
        for ( Runnable task; null != (task = deferredTasks.poll()); )
        {
            task.run();
        }
    }

    private final Map<Long, Delta<DynamicRecord>> schemata = new HashMap<>();
    private final Map<Long, Delta<NodeRecord>> nodes = new HashMap<>();
    private final Map<Long, Delta<RelationshipRecord>> relationships = new HashMap<>();
    private final Map<Long, Delta<PropertyRecord>> properties = new HashMap<>();
    private final Map<Long, Delta<DynamicRecord>> strings = new HashMap<>();
    private final Map<Long, Delta<DynamicRecord>> arrays = new HashMap<>();
    private final Map<Long, Delta<RelationshipTypeTokenRecord>> relationshipTypeTokens = new HashMap<>();
    private final Map<Long, Delta<LabelTokenRecord>> labelTokens = new HashMap<>();
    private final Map<Long, Delta<PropertyKeyTokenRecord>> propertyKeyTokens = new HashMap<>();
    private final Map<Long, Delta<DynamicRecord>> relationshipTypeNames = new HashMap<>();
    private final Map<Long, Delta<DynamicRecord>> nodeDynamicLabels = new HashMap<>();
    private final Map<Long, Delta<DynamicRecord>> labelNames = new HashMap<>();
    private final Map<Long, Delta<DynamicRecord>> propertyKeyNames = new HashMap<>();
    private final Map<Long, Delta<RelationshipGroupRecord>> relationshipGroups = new HashMap<>();
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
        records.put( record.getLongId(), new Delta<>( record ) );
        return record;
    }

    private static <R extends AbstractBaseRecord> void add( Map<Long, Delta<R>> records, R oldRecord, R newRecord )
    {
        records.put( newRecord.getLongId(), new Delta<>( oldRecord, newRecord ) );
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
            this.graph = new Delta<>( (NeoStoreRecord) oldRecord, (NeoStoreRecord) newRecord );
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
            this.graph = new Delta<>( (NeoStoreRecord) record );
        }
        else if ( record instanceof RelationshipGroupRecord )
        {
            add( relationshipGroups, (RelationshipGroupRecord) record );
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
        return new DirectRecordReference<>( record( records, id, version ), this );
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
    public RecordReference<RelationshipGroupRecord> relationshipGroup( long id )
    {
        return reference( relationshipGroups, id, Version.LATEST );
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

    @Override
    public RelationshipGroupRecord changedRelationshipGroup( long id )
    {
        return record( relationshipGroups, id, Version.NEW );
    }
}
