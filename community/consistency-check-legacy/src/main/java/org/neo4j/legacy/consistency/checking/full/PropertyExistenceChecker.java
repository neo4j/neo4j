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
package org.neo4j.legacy.consistency.checking.full;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.kernel.impl.store.record.NodePropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.PropertyConstraintRule;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.legacy.consistency.checking.CheckDecorator;
import org.neo4j.legacy.consistency.checking.CheckerEngine;
import org.neo4j.legacy.consistency.checking.ComparativeRecordChecker;
import org.neo4j.legacy.consistency.checking.OwningRecordCheck;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;

import static org.neo4j.legacy.consistency.checking.full.NodeLabelReader.getListOfLabels;

public class PropertyExistenceChecker extends CheckDecorator.Adapter
{
    private static final Class<PropertyConstraintRule> BASE_RULE = PropertyConstraintRule.class;

    private final PrimitiveIntObjectMap<int[]> nodes = Primitive.intObjectMap();
    private final PrimitiveIntObjectMap<int[]> relationships = Primitive.intObjectMap();

    public PropertyExistenceChecker( RecordStore<DynamicRecord> schemaStore )
    {
        SchemaStorage schemaStorage = new SchemaStorage( schemaStore );
        for ( Iterator<PropertyConstraintRule> rules = schemaStorage.schemaRules( BASE_RULE ); safeHasNext( rules ); )
        {
            PropertyConstraintRule rule = rules.next();

            PrimitiveIntObjectMap<int[]> storage;
            int labelOrRelType;
            int propertyKey;

            switch ( rule.getKind() )
            {
            case NODE_PROPERTY_EXISTENCE_CONSTRAINT:
                storage = nodes;
                NodePropertyExistenceConstraintRule nodeRule = (NodePropertyExistenceConstraintRule) rule;
                labelOrRelType = nodeRule.getLabel();
                propertyKey = nodeRule.getPropertyKey();
                break;

            case RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT:
                storage = relationships;
                RelationshipPropertyExistenceConstraintRule relRule = (RelationshipPropertyExistenceConstraintRule) rule;
                labelOrRelType = relRule.getRelationshipType();
                propertyKey = relRule.getPropertyKey();
                break;

            default:
                continue;
            }

            recordConstraint( labelOrRelType, propertyKey, storage );
        }
    }

    private boolean safeHasNext( Iterator<?> iterator )
    {
        for (; ; )
        {
            try
            {
                return iterator.hasNext();
            }
            catch ( Exception e )
            {
                // ignore
            }
        }
    }

    private static void recordConstraint( int labelOrRelType, int propertyKey, PrimitiveIntObjectMap<int[]> storage )
    {
        int[] propertyKeys = storage.get( labelOrRelType );
        if ( propertyKeys == null )
        {
            propertyKeys = new int[]{propertyKey};
        }
        else
        {
            propertyKeys = Arrays.copyOf( propertyKeys, propertyKeys.length + 1 );
            propertyKeys[propertyKeys.length - 1] = propertyKey;
        }
        storage.put( labelOrRelType, propertyKeys );
    }

    @Override
    public OwningRecordCheck<NodeRecord,ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
            OwningRecordCheck<NodeRecord,ConsistencyReport.NodeConsistencyReport> checker )
    {
        if ( nodes.isEmpty() )
        {
            return checker;
        }
        else
        {
            return new NodeChecker( nodes, checker );
        }
    }

    @Override
    public OwningRecordCheck<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport>
    decorateRelationshipChecker(
            OwningRecordCheck<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        if ( relationships.isEmpty() )
        {
            return checker;
        }
        else
        {
            return new RelationshipChecker( relationships, checker );
        }
    }

    private static abstract class Checker<RECORD extends PrimitiveRecord,
            REPORT extends ConsistencyReport.PrimitiveConsistencyReport>
            implements OwningRecordCheck<RECORD,REPORT>
    {
        private final OwningRecordCheck<RECORD,REPORT> next;

        private Checker( OwningRecordCheck<RECORD,REPORT> next )
        {
            this.next = next;
        }

        @Override
        public void check( RECORD record, CheckerEngine<RECORD,REPORT> engine, RecordAccess records )
        {
            next.check( record, engine, records );
            if ( record.inUse() )
            {
                PrimitiveIntSet keys = mandatoryPropertiesFor( record, engine, records );
                if ( keys != null )
                {
                    checkProperties( record, keys, engine, records );
                }
            }
        }

        private void checkProperties( RECORD record, PrimitiveIntSet keys, CheckerEngine<RECORD,REPORT> engine,
                RecordAccess records )
        {
            long firstProperty = record.getNextProp();
            if ( !Record.NO_NEXT_PROPERTY.is( firstProperty ) )
            {
                engine.comparativeCheck( records.property( firstProperty ), new ChainCheck<RECORD,REPORT>( keys ) );
            }
            else
            {
                reportMissingKeys( engine.report(), keys );
            }
        }

        abstract PrimitiveIntSet mandatoryPropertiesFor( RECORD record, CheckerEngine<RECORD,REPORT> engine,
                RecordAccess records );


        @Override
        public ComparativeRecordChecker<RECORD,PrimitiveRecord,REPORT> ownerCheck()
        {
            return next.ownerCheck();
        }

        @Override
        public void checkChange( RECORD oldRecord, RECORD newRecord, CheckerEngine<RECORD,REPORT> engine,
                DiffRecordAccess records )
        {
            next.checkChange( oldRecord, newRecord, engine, records );
        }
    }

    static class NodeChecker extends Checker<NodeRecord,ConsistencyReport.NodeConsistencyReport>
    {
        private final PrimitiveIntObjectMap<int[]> mandatory;
        private final int capacity;

        private NodeChecker( PrimitiveIntObjectMap<int[]> mandatory,
                OwningRecordCheck<NodeRecord,ConsistencyReport.NodeConsistencyReport> next )
        {
            super( next );
            this.mandatory = mandatory;
            this.capacity = SizeCounter.countSizeOf( mandatory );
        }

        @Override
        PrimitiveIntSet mandatoryPropertiesFor( NodeRecord record,
                CheckerEngine<NodeRecord,ConsistencyReport.NodeConsistencyReport> engine, RecordAccess records )
        {
            PrimitiveIntSet keys = null;
            for ( Long label : getListOfLabels( record, records, engine ) )
            {
                int[] propertyKeys = mandatory.get( label.intValue() );
                if ( propertyKeys != null )
                {
                    if ( keys == null )
                    {
                        keys = Primitive.intSet( capacity );
                    }
                    for ( int key : propertyKeys )
                    {
                        keys.add( key );
                    }
                }
            }
            return keys;
        }
    }

    static class RelationshipChecker extends Checker<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport>
    {
        private final PrimitiveIntObjectMap<int[]> mandatory;

        private RelationshipChecker( PrimitiveIntObjectMap<int[]> mandatory,
                OwningRecordCheck<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> next )
        {
            super( next );
            this.mandatory = mandatory;
        }

        @Override
        PrimitiveIntSet mandatoryPropertiesFor( RelationshipRecord record,
                CheckerEngine<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> engine,
                RecordAccess records )
        {
            int[] propertyKeys = mandatory.get( record.getType() );
            if ( propertyKeys != null )
            {
                PrimitiveIntSet keys = Primitive.intSet( propertyKeys.length );
                for ( int key : propertyKeys )
                {
                    keys.add( key );
                }
                return keys;
            }
            return null;
        }
    }

    static void reportMissingKeys( ConsistencyReport.PrimitiveConsistencyReport report, PrimitiveIntSet keys )
    {
        for ( PrimitiveIntIterator key = keys.iterator(); key.hasNext(); )
        {
            report.missingMandatoryProperty( key.next() );
        }
    }

    private static class ChainCheck<RECORD extends PrimitiveRecord,
            REPORT extends ConsistencyReport.PrimitiveConsistencyReport>
            implements ComparativeRecordChecker<RECORD,PropertyRecord,REPORT>
    {
        private static final int MAX_BLOCK_PER_RECORD_COUNT = 4;
        private final PrimitiveIntSet keys;

        public ChainCheck( PrimitiveIntSet keys )
        {
            this.keys = keys;
        }

        @Override
        public void checkReference( RECORD record, PropertyRecord property, CheckerEngine<RECORD,REPORT> engine,
                RecordAccess records )
        {
            for ( int key : keys( property ) )
            {
                keys.remove( key );
            }
            if ( Record.NO_NEXT_PROPERTY.is( property.getNextProp() ) )
            {
                if ( !keys.isEmpty() )
                {
                    reportMissingKeys( engine.report(), keys );
                }
            }
        }

        static int[] keys( PropertyRecord property )
        {
            int[] toStartWith = new int[MAX_BLOCK_PER_RECORD_COUNT];
            int index = 0;
            for ( PropertyBlock propertyBlock : property )
            {
                toStartWith[index++] = propertyBlock.getKeyIndexId();
            }
            return Arrays.copyOf( toStartWith, index );
        }
    }

    private static class SizeCounter implements PrimitiveIntObjectVisitor<int[],RuntimeException>
    {
        static int countSizeOf( PrimitiveIntObjectMap<int[]> map )
        {
            SizeCounter counter = new SizeCounter();
            map.visitEntries( counter );
            return counter.size;
        }

        private int size;

        @Override
        public boolean visited( int key, int[] ints ) throws RuntimeException
        {
            size += ints.length;
            return false;
        }
    }
}
