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
package org.neo4j.kernel.impl.store;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.schema_new.SchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaDescriptorPredicates;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptor;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.storageengine.api.schema.SchemaRule;

public class SchemaStorage implements SchemaRuleAccess
{
    public enum IndexRuleKind implements Predicate<IndexRule>
    {
        INDEX
        {
            @Override
            public boolean test( IndexRule rule )
            {
                return !rule.canSupportUniqueConstraint();
            }
        },
        CONSTRAINT
        {
            @Override
            public boolean test( IndexRule rule )
            {
                return rule.canSupportUniqueConstraint();
            }
        },
        ALL
        {
            @Override
            public boolean test( IndexRule rule )
            {
                return true;
            }
        }
    }

    private final RecordStore<DynamicRecord> schemaStore;

    public SchemaStorage( RecordStore<DynamicRecord> schemaStore )
    {
        this.schemaStore = schemaStore;
    }

    /**
     * Find the IndexRule, of any kind, for the given SchemaDescriptor.
     *
     * @return  the matching IndexRule, or null if no matching IndexRule was found
     * @throws  IllegalStateException if more than one matching candidate rule.
     */
    public IndexRule indexRule( SchemaDescriptor descriptor )
    {
        return indexRule( descriptor, IndexRuleKind.ALL );
    }

    /**
     * Find the IndexRule that matches both the given SchemaDescriptor and passed the filter.
     *
     * @return  the matching IndexRule, or null if no matching IndexRule was found
     * @throws  IllegalStateException if more than one matching candidate rule.
     */
    public IndexRule indexRule( final SchemaDescriptor descriptor, Predicate<IndexRule> filter )
    {
        Iterator<IndexRule> rules = schemaRules( cast( IndexRule.class ), IndexRule.class,
                rule -> rule.getSchemaDescriptor().equals( descriptor ) );

        IndexRule foundRule = null;

        while ( rules.hasNext() )
        {
            IndexRule candidate = rules.next();
            if ( filter.test( candidate ) )
            {
                if ( foundRule != null )
                {
                    throw new IllegalStateException( String.format(
                            "Found more than one matching index rule, %s and %s", foundRule, candidate ) );
                }
                foundRule = candidate;
            }
        }

        return foundRule;
    }

    public Iterator<IndexRule> allIndexRules()
    {
        return schemaRules( IndexRule.class );
    }

    public Iterator<ConstraintRule> allConstraintRules()
    {
        return schemaRules( ConstraintRule.class );
    }

    public <R> Iterator<R> mapConstraintRulesFor(
            Predicate<ConstraintRule> predicate, Function<ConstraintRule, R> map )
    {
        return schemaRules( map, ConstraintRule.class, predicate );
    }

    private <R extends SchemaRule, T> Iterator<T> schemaRules(
            Function<? super R, T> conversion, final Class<R> ruleType, final Predicate<R> predicate )
    {
        @SuppressWarnings("unchecked"/*the predicate ensures that this is safe*/)
        Function<SchemaRule, T> ruleConversion = (Function) conversion;
        return Iterators.map( ruleConversion, Iterators
                .filter( rule -> ruleType.isInstance( rule ) && predicate.test( (R) rule ), loadAllSchemaRules() ) );
    }

    private <R extends SchemaRule> Iterator<R> schemaRules( final Class<R> ruleClass )
    {
        @SuppressWarnings({"UnnecessaryLocalVariable", "unchecked"/*the predicate ensures that this cast is safe*/})
        Iterator<R> result = (Iterator) Iterators.filter( ruleClass::isInstance, loadAllSchemaRules() );
        return result;
    }

    Iterator<SchemaRule> loadAllSchemaRules()
    {
        return new PrefetchingIterator<SchemaRule>()
        {
            private final long highestId = schemaStore.getHighestPossibleIdInUse();
            private long currentId = 1; /*record 0 contains the block size*/
            private final byte[] scratchData = newRecordBuffer();
            private final DynamicRecord record = schemaStore.newRecord();

            @Override
            protected SchemaRule fetchNextOrNull()
            {
                while ( currentId <= highestId )
                {
                    long id = currentId++;
                    schemaStore.getRecord( id, record, RecordLoad.FORCE );
                    if ( record.inUse() && record.isStartRecord() )
                    {
                        try
                        {
                            return getSchemaRule( id, scratchData );
                        }
                        catch ( MalformedSchemaRuleException e )
                        {
                            // TODO remove this and throw this further up
                            throw new RuntimeException( e );
                        }
                    }
                }
                return null;
            }
        };
    }

    @Override
    public SchemaRule loadSingleSchemaRule( long ruleId ) throws MalformedSchemaRuleException
    {
        return getSchemaRule( ruleId, newRecordBuffer() );
    }

    private byte[] newRecordBuffer()
    {
        return new byte[schemaStore.getRecordSize()*4];
    }

    private SchemaRule getSchemaRule( long id, byte[] buffer ) throws MalformedSchemaRuleException
    {
        Collection<DynamicRecord> records;
        try
        {
            records = schemaStore.getRecords( id, RecordLoad.NORMAL );
        }
        catch ( Exception e )
        {
            throw new MalformedSchemaRuleException( e.getMessage(), e );
        }
        return SchemaStore.readSchemaRule( id, records, buffer );
    }

    public long newRuleId()
    {
        return schemaStore.nextId();
    }

    public ConstraintRule singleConstraintRule( final ConstraintDescriptor descriptor )
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        Iterator<ConstraintRule> rules = schemaRules(
                cast( ConstraintRule.class ), ConstraintRule.class,
                item -> item.getConstraintDescriptor().equals( descriptor ) );

        if ( !rules.hasNext() )
        {
            throw new SchemaRuleNotFoundException( SchemaRule.Kind.map( descriptor ), descriptor.schema() );
        }

        ConstraintRule rule = rules.next();

        if ( rules.hasNext() )
        {
            throw new DuplicateSchemaRuleException( SchemaRule.Kind.map( descriptor ), descriptor.schema() );
        }
        return rule;
    }

    public Iterator<ConstraintRule> constraintRules( SchemaDescriptor descriptor )
    {
        return schemaRules(
                cast( ConstraintRule.class ), ConstraintRule.class,
                item -> item.getSchemaDescriptor().equals( descriptor ) );
    }

    private static <FROM, TO> Function<FROM,TO> cast( final Class<TO> to )
    {
        return new Function<FROM,TO>()
        {
            @Override
            public TO apply( FROM from )
            {
                return to.cast( from );
            }

            @Override
            public String toString()
            {
                return "cast(to=" + to.getName() + ")";
            }
        };
    }
}
