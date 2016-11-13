/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.consistency.checking.full;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.DynamicStore;
import org.neo4j.consistency.checking.OwningRecordCheck;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;

import static java.util.Collections.unmodifiableMap;
import static org.neo4j.consistency.RecordType.ARRAY_PROPERTY;
import static org.neo4j.consistency.RecordType.PROPERTY_KEY_NAME;
import static org.neo4j.consistency.RecordType.RELATIONSHIP_TYPE_NAME;
import static org.neo4j.consistency.RecordType.STRING_PROPERTY;

class OwnerCheck implements CheckDecorator
{
    private final ConcurrentMap<Long, PropertyOwner> owners;
    private final Map<RecordType, ConcurrentMap<Long, DynamicOwner>> dynamics;

    OwnerCheck( boolean active, DynamicStore... stores )
    {
        this.owners = active ? new ConcurrentHashMap<>( 16, 0.75f, 4 ) : null;
        this.dynamics = active ? initialize( stores ) : null;
    }

    private static Map<RecordType, ConcurrentMap<Long, DynamicOwner>> initialize( DynamicStore[] stores )
    {
        EnumMap<RecordType, ConcurrentMap<Long, DynamicOwner>> map =
                new EnumMap<>( RecordType.class );
        for ( DynamicStore store : stores )
        {
            map.put( store.type, new ConcurrentHashMap<>( 16, 0.75f, 4 ) );
        }
        return unmodifiableMap( map );
    }

    void scanForOrphanChains( ProgressMonitorFactory progressFactory )
    {
        List<Runnable> tasks = new ArrayList<>();
        ProgressMonitorFactory.MultiPartBuilder progress = progressFactory
                .multipleParts( "Checking for orphan chains" );
        if ( owners != null )
        {
            tasks.add( new OrphanCheck( RecordType.PROPERTY, owners, progress ) );
        }
        if ( dynamics != null )
        {
            for ( Map.Entry<RecordType, ConcurrentMap<Long, DynamicOwner>> entry : dynamics.entrySet() )
            {
                tasks.add( new OrphanCheck( entry.getKey(), entry.getValue(), progress ) );
            }
        }
        for ( Runnable task : tasks )
        {
            task.run();
        }
    }

    private static class OrphanCheck implements Runnable
    {
        private final ConcurrentMap<Long, ? extends Owner> owners;
        private final ProgressListener progress;

        OrphanCheck( RecordType property, ConcurrentMap<Long, ? extends Owner> owners,
                     ProgressMonitorFactory.MultiPartBuilder progress )
        {
            this.owners = owners;
            this.progress = progress.progressForPart( "Checking for orphan " + property.name() + " chains",
                                                      owners.size() );
        }

        @Override
        public void run()
        {
            for ( Owner owner : owners.values() )
            {
                owner.checkOrphanage();
                progress.add( 1 );
            }
            progress.done();
        }
    }

    @Override
    public void prepare()
    {
    }

    @Override
    public OwningRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
            OwningRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new PrimitiveCheckerDecorator<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport>( checker )
        {
            @Override
            PropertyOwner owner( NeoStoreRecord record )
            {
                return PropertyOwner.OWNING_GRAPH;
            }
        };
    }

    @Override
    public OwningRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
            OwningRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new PrimitiveCheckerDecorator<NodeRecord, ConsistencyReport.NodeConsistencyReport>( checker )
        {
            @Override
            PropertyOwner owner( NodeRecord record )
            {
                return new PropertyOwner.OwningNode( record );
            }
        };
    }

    @Override
    public OwningRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
            OwningRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new PrimitiveCheckerDecorator<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>(
                checker )
        {
            @Override
            PropertyOwner owner( RelationshipRecord record )
            {
                return new PropertyOwner.OwningRelationship( record );
            }
        };
    }

    @Override
    public RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
            final RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
        if ( owners == null && dynamics == null )
        {
            return checker;
        }
        return new RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>()
        {
            @Override
            public void check( PropertyRecord record,
                               CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                               RecordAccess records )
            {
                if ( record.inUse() )
                {
                    if ( owners != null && Record.NO_PREVIOUS_PROPERTY.is( record.getPrevProp() ) )
                    { // this record is first in a chain
                        PropertyOwner.UnknownOwner owner = new PropertyOwner.UnknownOwner();
                        engine.comparativeCheck( owner, ORPHAN_CHECKER );
                        if ( null == owners.putIfAbsent( record.getId(), owner ) )
                        {
                            owner.markInCustody();
                        }
                    }
                    if ( dynamics != null )
                    {
                        for ( PropertyBlock block : record )
                        {
                            RecordType type = recordType( block.forceGetType() );
                            if ( type != null )
                            {
                                ConcurrentMap<Long, DynamicOwner> dynamicOwners = dynamics.get( type );
                                if ( dynamicOwners != null )
                                {
                                    long id = block.getSingleValueLong();
                                    DynamicOwner.Property owner = new DynamicOwner.Property( type, record );
                                    DynamicOwner prev = dynamicOwners.put( id, owner );
                                    if ( prev != null )
                                    {
                                        engine.comparativeCheck( prev.record( records ), owner );
                                    }
                                }
                            }
                        }
                    }
                }
                checker.check( record, engine, records );
            }
        };
    }

    private RecordType recordType( PropertyType type )
    {
        if ( type == null )
        {
            return null;
        }

        switch ( type )
        {
        case STRING:
            return STRING_PROPERTY;
        case ARRAY:
            return ARRAY_PROPERTY;
        default:
            return null;
        }
    }

    @Override
    public RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> decoratePropertyKeyTokenChecker(
            RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
    {
        ConcurrentMap<Long, DynamicOwner> dynamicOwners = dynamicOwners( PROPERTY_KEY_NAME );
        if ( dynamicOwners == null )
        {
            return checker;
        }
        return new NameCheckerDecorator
                <PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport>( checker, dynamicOwners )
        {
            @Override
            DynamicOwner.NameOwner owner( PropertyKeyTokenRecord record )
            {
                return new DynamicOwner.PropertyKey( record );
            }
        };
    }

    @Override
    public RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> decorateRelationshipTypeTokenChecker(
            RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> checker )
    {
        ConcurrentMap<Long, DynamicOwner> dynamicOwners = dynamicOwners( RELATIONSHIP_TYPE_NAME );
        if ( dynamicOwners == null )
        {
            return checker;
        }
        return new NameCheckerDecorator
                <RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport>( checker, dynamicOwners )
        {
            @Override
            DynamicOwner.NameOwner owner( RelationshipTypeTokenRecord record )
            {
                return new DynamicOwner.RelationshipTypeToken( record );
            }
        };
    }

    @Override
    public RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> decorateLabelTokenChecker(
            RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> checker )
    {
        ConcurrentMap<Long, DynamicOwner> dynamicOwners = dynamicOwners( RELATIONSHIP_TYPE_NAME );
        if ( dynamicOwners == null )
        {
            return checker;
        }
        return new NameCheckerDecorator<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport>( checker, dynamicOwners )
        {
            @Override
            DynamicOwner.NameOwner owner( LabelTokenRecord record )
            {
                return new DynamicOwner.LabelToken( record );
            }
        };
    }

    @Override
    public RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> decorateLabelMatchChecker(
            RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> checker )
    {
        // TODO: Understand what this does.
        return checker;
    }

    RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> decorateDynamicChecker(
            final RecordType type, final RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
    {
        final ConcurrentMap<Long, DynamicOwner> dynamicOwners = dynamicOwners( type );
        if ( dynamicOwners == null )
        {
            return checker;
        }
        return new RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport>()
        {
            @Override
            public void check( DynamicRecord record,
                               CheckerEngine<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> engine,
                               RecordAccess records )
            {
                if ( record.inUse() )
                {
                    DynamicOwner.Unknown owner = new DynamicOwner.Unknown();
                    engine.comparativeCheck( owner, DynamicOwner.ORPHAN_CHECK );
                    if ( null == dynamicOwners.putIfAbsent( record.getId(), owner ) )
                    {
                        owner.markInCustody();
                    }
                    if ( !Record.NO_NEXT_BLOCK.is( record.getNextBlock() ) )
                    {
                        DynamicOwner.Dynamic nextOwner = new DynamicOwner.Dynamic( type, record );
                        DynamicOwner prevOwner = dynamicOwners.put( record.getNextBlock(), nextOwner );
                        if ( prevOwner != null )
                        {
                            engine.comparativeCheck( prevOwner.record( records ), nextOwner );
                        }
                    }
                }
                checker.check( record, engine, records );
            }
        };
    }

    @Override
    public RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> decorateRelationshipGroupChecker(
            RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> checker )
    {
        return checker;
    }

    private ConcurrentMap<Long, DynamicOwner> dynamicOwners( RecordType type )
    {
        return dynamics == null ? null : dynamics.get( type );
    }

    private abstract class PrimitiveCheckerDecorator<RECORD extends PrimitiveRecord,
            REPORT extends ConsistencyReport.PrimitiveConsistencyReport>
            implements OwningRecordCheck<RECORD, REPORT>
    {
        private final OwningRecordCheck<RECORD, REPORT> checker;

        PrimitiveCheckerDecorator( OwningRecordCheck<RECORD, REPORT> checker )
        {
            this.checker = checker;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void check( RECORD record, CheckerEngine<RECORD, REPORT> engine, RecordAccess records )
        {
            if ( record.inUse() )
            {
                long prop = record.getNextProp();
                if ( !Record.NO_NEXT_PROPERTY.is( prop ) )
                {
                    PropertyOwner previous = owners.put( prop, owner( record ) );
                    if ( previous != null )
                    {
                        engine.comparativeCheck( previous.record( records ), checker.ownerCheck() );
                    }
                }
            }
            checker.check( record, engine, records );
        }

        @Override
        public ComparativeRecordChecker<RECORD,PrimitiveRecord,REPORT> ownerCheck()
        {
            return checker.ownerCheck();
        }

        abstract PropertyOwner owner( RECORD record );
    }

    private abstract static class NameCheckerDecorator
            <RECORD extends TokenRecord, REPORT extends ConsistencyReport.NameConsistencyReport>
            implements RecordCheck<RECORD, REPORT>
    {
        private final RecordCheck<RECORD, REPORT> checker;
        private final ConcurrentMap<Long, DynamicOwner> owners;

        public NameCheckerDecorator( RecordCheck<RECORD, REPORT> checker, ConcurrentMap<Long, DynamicOwner> owners )
        {
            this.checker = checker;
            this.owners = owners;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void check( RECORD record, CheckerEngine<RECORD, REPORT> engine, RecordAccess records )
        {
            if ( record.inUse() )
            {
                DynamicOwner.NameOwner owner = owner( record );
                DynamicOwner prev = owners.put( (long)record.getNameId(), owner );
                if ( prev != null )
                {
                    engine.comparativeCheck( prev.record( records ), owner );
                }
            }
            checker.check( record, engine, records );
        }

        abstract DynamicOwner.NameOwner owner( RECORD record );
    }

    private static final ComparativeRecordChecker<PropertyRecord, PrimitiveRecord, ConsistencyReport.PropertyConsistencyReport> ORPHAN_CHECKER =
            ( record, primitiveRecord, engine, records ) -> engine.report().orphanPropertyChain();
}
