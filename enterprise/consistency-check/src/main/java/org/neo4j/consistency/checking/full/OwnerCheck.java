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
package org.neo4j.consistency.checking.full;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.DynamicStore;
import org.neo4j.consistency.checking.PrimitiveRecordCheck;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.TokenRecord;

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
        this.owners = active ? new ConcurrentHashMap<Long, PropertyOwner>( 16, 0.75f, 4 ) : null;
        this.dynamics = active ? initialize( stores ) : null;
    }

    private static Map<RecordType, ConcurrentMap<Long, DynamicOwner>> initialize( DynamicStore[] stores )
    {
        EnumMap<RecordType, ConcurrentMap<Long, DynamicOwner>> map =
                new EnumMap<>( RecordType.class );
        for ( DynamicStore store : stores )
        {
            map.put( store.type, new ConcurrentHashMap<Long, DynamicOwner>( 16, 0.75f, 4 ) );
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
    public RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
            PrimitiveRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new PrimitiveCheckerDecorator<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport>( checker )
        {
            PropertyOwner owner( NeoStoreRecord record )
            {
                return PropertyOwner.OWNING_GRAPH;
            }
        };
    }

    @Override
    public RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
            PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new PrimitiveCheckerDecorator<NodeRecord, ConsistencyReport.NodeConsistencyReport>( checker )
        {
            PropertyOwner owner( NodeRecord record )
            {
                return new PropertyOwner.OwningNode( record );
            }
        };
    }

    @Override
    public RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
            PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        if ( owners == null )
        {
            return checker;
        }
        return new PrimitiveCheckerDecorator<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>(
                checker )
        {
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
            public void check( PropertyRecord record, ConsistencyReport.PropertyConsistencyReport report,
                               RecordAccess records )
            {
                if ( record.inUse() )
                {
                    if ( owners != null && Record.NO_PREVIOUS_PROPERTY.is( record.getPrevProp() ) )
                    { // this record is first in a chain
                        PropertyOwner.UnknownOwner owner = new PropertyOwner.UnknownOwner();
                        report.forReference( owner, ORPHAN_CHECKER );
                        if ( null == owners.putIfAbsent( record.getId(), owner ) )
                        {
                            owner.markInCustody();
                        }
                    }
                    if ( dynamics != null )
                    {
                        List<PropertyBlock> blocks = record.getPropertyBlocks();
                        for ( PropertyBlock block : blocks )
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
                                        report.forReference( prev.record( records ), owner );
                                    }
                                }
                            }
                        }
                    }
                }
                checker.check( record, report, records );
            }

            @Override
            public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                                     ConsistencyReport.PropertyConsistencyReport report, DiffRecordAccess records )
            {
                checker.checkChange( oldRecord, newRecord, report, records );
            }
        };
    }

    private RecordType recordType( PropertyType type )
    {
        if ( type != null )
        {
            switch ( type )
            {
            case STRING:
                return STRING_PROPERTY;
            case ARRAY:
                return ARRAY_PROPERTY;
            }
        }
        return null;
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
        return new NameCheckerDecorator
                <LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport>( checker, dynamicOwners )
        {
            @Override
            DynamicOwner.NameOwner owner( LabelTokenRecord record )
            {
                return new DynamicOwner.LabelToken( record );
            }
        };
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
            public void check( DynamicRecord record, ConsistencyReport.DynamicConsistencyReport report,
                               RecordAccess records )
            {
                if ( record.inUse() )
                {
                    DynamicOwner.Unknown owner = new DynamicOwner.Unknown();
                    report.forReference( owner, DynamicOwner.ORPHAN_CHECK );
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
                            report.forReference( prevOwner.record( records ), nextOwner );
                        }
                    }
                }
                checker.check( record, report, records );
            }

            @Override
            public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord,
                                     ConsistencyReport.DynamicConsistencyReport report, DiffRecordAccess records )
            {
                checker.checkChange( oldRecord, newRecord, report, records );
            }
        };
    }

    private ConcurrentMap<Long, DynamicOwner> dynamicOwners( RecordType type )
    {
        return dynamics == null ? null : dynamics.get( type );
    }

    private abstract class PrimitiveCheckerDecorator<RECORD extends PrimitiveRecord,
            REPORT extends ConsistencyReport.PrimitiveConsistencyReport<RECORD, REPORT>>
            implements RecordCheck<RECORD, REPORT>
    {
        private final PrimitiveRecordCheck<RECORD, REPORT> checker;

        PrimitiveCheckerDecorator( PrimitiveRecordCheck<RECORD, REPORT> checker )
        {
            this.checker = checker;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void check( RECORD record, REPORT report, RecordAccess records )
        {
            if ( record.inUse() )
            {
                long prop = record.getNextProp();
                if ( !Record.NO_NEXT_PROPERTY.is( prop ) )
                {
                    PropertyOwner previous = owners.put( prop, owner( record ) );
                    if ( previous != null )
                    {
                        report.forReference( previous.record( records ), checker.ownerCheck );
                    }
                }
            }
            checker.check( record, report, records );
        }

        @Override
        public void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, DiffRecordAccess records )
        {
            checker.checkChange( oldRecord, newRecord, report, records );
        }

        abstract PropertyOwner owner( RECORD record );
    }

    private static abstract class NameCheckerDecorator
            <RECORD extends TokenRecord, REPORT extends ConsistencyReport.NameConsistencyReport<RECORD, REPORT>>
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
        public void check( RECORD record, REPORT report, RecordAccess records )
        {
            if ( record.inUse() )
            {
                DynamicOwner.NameOwner owner = owner( record );
                DynamicOwner prev = owners.put( (long)record.getNameId(), owner );
                if ( prev != null )
                {
                    report.forReference( prev.record( records ), owner );
                }
            }
            checker.check( record, report, records );
        }

        abstract DynamicOwner.NameOwner owner( RECORD record );

        @Override
        public void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, DiffRecordAccess records )
        {
            checker.checkChange( oldRecord, newRecord, report, records );
        }
    }

    private static final ComparativeRecordChecker<PropertyRecord, PrimitiveRecord, ConsistencyReport.PropertyConsistencyReport> ORPHAN_CHECKER =
            new ComparativeRecordChecker<PropertyRecord, PrimitiveRecord, ConsistencyReport.PropertyConsistencyReport>()
            {
                @Override
                public void checkReference( PropertyRecord record, PrimitiveRecord primitiveRecord,
                                            ConsistencyReport.PropertyConsistencyReport report, RecordAccess records )
                {
                    report.orphanPropertyChain();
                }
            };
}
