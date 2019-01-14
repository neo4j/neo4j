/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.consistency.checking.full;

import java.util.Arrays;
import java.util.function.Function;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.helpers.Numbers.safeCastLongToInt;

public class MandatoryProperties
{
    private final PrimitiveIntObjectMap<int[]> nodes = Primitive.intObjectMap();
    private final PrimitiveIntObjectMap<int[]> relationships = Primitive.intObjectMap();
    private final StoreAccess storeAccess;

    public MandatoryProperties( StoreAccess storeAccess )
    {
        this.storeAccess = storeAccess;
        SchemaStorage schemaStorage = new SchemaStorage( storeAccess.getSchemaStore() );
        for ( ConstraintRule rule : constraintsIgnoringMalformed( schemaStorage ) )
        {
            if ( rule.getConstraintDescriptor().enforcesPropertyExistence() )
            {
                rule.schema().processWith( constraintRecorder );
            }
        }
    }

    private SchemaProcessor constraintRecorder = new SchemaProcessor()
    {
        @Override
        public void processSpecific( LabelSchemaDescriptor schema )
        {
            for ( int propertyId : schema.getPropertyIds() )
            {
                recordConstraint( schema.getLabelId(), propertyId, nodes );
            }
        }

        @Override
        public void processSpecific( RelationTypeSchemaDescriptor schema )
        {
            for ( int propertyId : schema.getPropertyIds() )
            {
                recordConstraint( schema.getRelTypeId(), propertyId, relationships );
            }
        }
    };

    public Function<NodeRecord,Check<NodeRecord,ConsistencyReport.NodeConsistencyReport>> forNodes(
            final ConsistencyReporter reporter )
    {
        return node ->
        {
            PrimitiveIntSet keys = null;
            for ( long labelId : NodeLabelReader.getListOfLabels( node, storeAccess.getNodeDynamicLabelStore() ) )
            {
                // labelId _is_ actually an int. A technical detail in the store format has these come in a long[]
                int[] propertyKeys = nodes.get( safeCastLongToInt( labelId ) );
                if ( propertyKeys != null )
                {
                    if ( keys == null )
                    {
                        keys = Primitive.intSet( 16 );
                    }
                    for ( int key : propertyKeys )
                    {
                        keys.add( key );
                    }
                }
            }
            return keys != null
                    ? new RealCheck<>( node, ConsistencyReport.NodeConsistencyReport.class, reporter,
                            RecordType.NODE, keys )
                    : MandatoryProperties.noCheck();
        };
    }

    public Function<RelationshipRecord,Check<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport>>
            forRelationships( final ConsistencyReporter reporter )
    {
        return relationship ->
        {
            int[] propertyKeys = relationships.get( relationship.getType() );
            if ( propertyKeys != null )
            {
                PrimitiveIntSet keys = Primitive.intSet( propertyKeys.length );
                for ( int key : propertyKeys )
                {
                    keys.add( key );
                }
                return new RealCheck<>( relationship, ConsistencyReport.RelationshipConsistencyReport.class,
                        reporter, RecordType.RELATIONSHIP, keys );
            }
            return noCheck();
        };
    }

    private Iterable<ConstraintRule> constraintsIgnoringMalformed( SchemaStorage schemaStorage )
    {
        return schemaStorage::constraintsGetAllIgnoreMalformed;
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

    public interface Check<RECORD extends PrimitiveRecord,REPORT extends ConsistencyReport.PrimitiveConsistencyReport>
            extends AutoCloseable
    {
        void receive( int[] keys );

        @Override
        void close();
    }

    @SuppressWarnings( "unchecked" )
    private static <RECORD extends PrimitiveRecord,
            REPORT extends ConsistencyReport.PrimitiveConsistencyReport> Check<RECORD,REPORT> noCheck()
    {
        return NONE;
    }

    @SuppressWarnings( "rawtypes" )
    private static final Check NONE = new Check()
    {
        @Override
        public void receive( int[] keys )
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public String toString()
        {
            return "NONE";
        }
    };

    private static class RealCheck<RECORD extends PrimitiveRecord,REPORT extends ConsistencyReport.PrimitiveConsistencyReport>
            implements Check<RECORD,REPORT>
    {
        private final RECORD record;
        private final PrimitiveIntSet mandatoryKeys;
        private final Class<REPORT> reportClass;
        private final ConsistencyReporter reporter;
        private final RecordType recordType;

        RealCheck( RECORD record, Class<REPORT> reportClass, ConsistencyReporter reporter, RecordType recordType,
                PrimitiveIntSet mandatoryKeys )
        {
            this.record = record;
            this.reportClass = reportClass;
            this.reporter = reporter;
            this.recordType = recordType;
            this.mandatoryKeys = mandatoryKeys;
        }

        @Override
        public void receive( int[] keys )
        {
            for ( int key : keys )
            {
                mandatoryKeys.remove( key );
            }
        }

        @Override
        public void close()
        {
            if ( !mandatoryKeys.isEmpty() )
            {
                for ( PrimitiveIntIterator key = mandatoryKeys.iterator(); key.hasNext(); )
                {
                    reporter.report( record, reportClass, recordType ).missingMandatoryProperty( key.next() );
                }
            }
        }

        @Override
        public String toString()
        {
            return "Mandatory properties: " + mandatoryKeys;
        }
    }
}
