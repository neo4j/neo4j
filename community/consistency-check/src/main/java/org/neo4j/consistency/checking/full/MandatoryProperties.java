/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodePropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.Utils;

public class MandatoryProperties
{
    private static final Class<PropertyConstraintRule> BASE_RULE = PropertyConstraintRule.class;

    private final PrimitiveIntObjectMap<int[]> nodes = Primitive.intObjectMap();
    private final PrimitiveIntObjectMap<int[]> relationships = Primitive.intObjectMap();
    private final StoreAccess storeAccess;

    public MandatoryProperties( StoreAccess storeAccess )
    {
        this.storeAccess = storeAccess;
        SchemaStorage schemaStorage = new SchemaStorage( storeAccess.getSchemaStore() );

        for ( PropertyConstraintRule rule : constraintsIgnoringMalformed( schemaStorage ) )
        {
            switch ( rule.getKind() )
            {
            case NODE_PROPERTY_EXISTENCE_CONSTRAINT:
                NodePropertyExistenceConstraintRule nodeRule = (NodePropertyExistenceConstraintRule) rule;
                recordConstraint( nodeRule.getLabel(), nodeRule.descriptor().getPropertyKeyId(), nodes );
                break;

            case RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT:
                RelationshipPropertyExistenceConstraintRule relRule = (RelationshipPropertyExistenceConstraintRule) rule;
                recordConstraint( relRule.getRelationshipType(), relRule.getPropertyKey(), relationships );
                break;

            default:
                continue;
            }
        }
    }

    public Function<NodeRecord,Check<NodeRecord,ConsistencyReport.NodeConsistencyReport>> forNodes(
            final ConsistencyReporter reporter )
    {
        return node -> {
            PrimitiveIntSet keys = null;
            for ( long labelId : NodeLabelReader.getListOfLabels( node, storeAccess.getNodeDynamicLabelStore() ) )
            {
                // labelId _is_ actually an int. A technical detail in the store format has these come in a long[]
                int[] propertyKeys = nodes.get( Utils.safeCastLongToInt( labelId ) );
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
                    : MandatoryProperties.<NodeRecord,ConsistencyReport.NodeConsistencyReport>noCheck();
        };
    }

    public Function<RelationshipRecord,Check<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport>>
            forRelationships( final ConsistencyReporter reporter )
    {
        return relationship -> {
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

    /**
     * The iterators returned by the SchemaStorage will throw a `MalformedSchemaRuleException` wrapped in a
     * `RuntimeException` if it encounters malformed schema rules. As the iterator implementation is prefetching this
     * will manifest in the hasNext() call. Attempting again will actually attempt to read the schema rule after the
     * malformed one. This method will therefore execute `hasNext()` until 1) it reads a complete schema rule, 2) all
     * schema rules are read, or 3) it get's an exception that is not wrapping a `MalformedSchemaRuleException`.
     */
    private Iterable<PropertyConstraintRule> constraintsIgnoringMalformed( SchemaStorage schemaStorage )
    {
        final Iterator<PropertyConstraintRule> ruleIterator = schemaStorage.schemaRules( BASE_RULE );
        return () -> new Iterator<PropertyConstraintRule>()
            {
                @Override
                public boolean hasNext()
                {
                    while ( true )
                    {
                        try
                        {
                            return ruleIterator.hasNext();
                        }
                        catch ( Exception e )
                        {
                            if ( !MalformedSchemaRuleException.class.isInstance( e.getCause() ) )
                            {
                                throw e;
                            }
                        }
                    }
                }

                @Override
                public PropertyConstraintRule next()
                {
                    return ruleIterator.next();
                }
            };
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
    private static <RECORD extends PrimitiveRecord,REPORT extends ConsistencyReport.PrimitiveConsistencyReport> Check<RECORD,REPORT>noCheck()
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
