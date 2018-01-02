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
package org.neo4j.kernel.impl.store;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.exceptions.schema.DuplicateEntitySchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.EntitySchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodePropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.NodePropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;

import static org.neo4j.function.Functions.cast;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;

public class SchemaStorage implements SchemaRuleAccess
{
    public enum IndexRuleKind
    {
        INDEX
                {
                    @Override
                    public boolean isOfKind( IndexRule rule )
                    {
                        return !rule.isConstraintIndex();
                    }
                },
        CONSTRAINT
                {
                    @Override
                    public boolean isOfKind( IndexRule rule )
                    {
                        return rule.isConstraintIndex();
                    }
                },
        ALL
                {
                    @Override
                    public boolean isOfKind( IndexRule rule )
                    {
                        return true;
                    }
                };

        public abstract boolean isOfKind( IndexRule rule );
    }

    private final RecordStore<DynamicRecord> schemaStore;

    public SchemaStorage( RecordStore<DynamicRecord> schemaStore )
    {
        this.schemaStore = schemaStore;
    }

    /**
     * Find the IndexRule, of any kind, for the given label and property key.
     *
     * Otherwise throw if there are not exactly one matching candidate rule.
     */
    public IndexRule indexRule( int labelId, int propertyKeyId )
    {
        return indexRule( labelId, propertyKeyId, IndexRuleKind.ALL );
    }

    /**
     * Find and IndexRule of the given kind, for the given label and property.
     *
     * Otherwise throw if there are not exactly one matching candidate rule.
     */
    public IndexRule indexRule( final int labelId, final int propertyKeyId, IndexRuleKind kind )
    {
        Iterator<IndexRule> rules = schemaRules( cast( IndexRule.class ), IndexRule.class, new Predicate<IndexRule>()
        {
            @Override
            public boolean test( IndexRule rule )
            {
                return rule.getLabel() == labelId && rule.getPropertyKey() == propertyKeyId;
            }
        } );

        IndexRule foundRule = null;

        while ( rules.hasNext() )
        {
            IndexRule candidate = rules.next();
            if ( kind.isOfKind( candidate ) )
            {
                if ( foundRule != null )
                {
                    throw new ThisShouldNotHappenError( "Jake", String.format("Found more than one matching index rule, %s and %s", foundRule, candidate) );
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

    public <R extends NodePropertyConstraintRule, T> Iterator<T> schemaRulesForNodes(
            Function<? super R,T> conversion, Class<R> type, final int labelId, Predicate<R> predicate )
    {
        return schemaRules( conversion, type, Predicates.all( predicate, new Predicate<R>()
        {
            @Override
            public boolean test( R rule )
            {
                return rule.getLabel() == labelId;
            }
        } ) );
    }

    public <R extends RelationshipPropertyConstraintRule, T> Iterator<T> schemaRulesForRelationships(
            Function<? super R,T> conversion, Class<R> type, final int typeId, Predicate<R> predicate )
    {
        return schemaRules( conversion, type, Predicates.all( predicate, new Predicate<R>()
        {
            @Override
            public boolean test( R rule )
            {
                return rule.getRelationshipType() == typeId;
            }
        } ) );
    }

    private <R extends SchemaRule, T> Iterator<T> schemaRules(
            Function<? super R, T> conversion, final Class<R> ruleType, final Predicate<R> predicate )
    {
        @SuppressWarnings("unchecked"/*the predicate ensures that this is safe*/)
        Function<SchemaRule, T> ruleConversion = (Function) conversion;
        return map( ruleConversion, filter( new Predicate<SchemaRule>()
        {
            @SuppressWarnings( "unchecked" )
            @Override
            public boolean test( SchemaRule rule )
            {
                return ruleType.isAssignableFrom( rule.getKind().getRuleClass() ) &&
                       predicate.test( (R) rule );
            }
        }, loadAllSchemaRules() ) );
    }

    public <R extends SchemaRule, T> Iterator<T> schemaRules(
            Function<? super R, T> conversion, final Class<R> ruleClass )
    {
        @SuppressWarnings("unchecked"/*the predicate ensures that this is safe*/)
        Function<SchemaRule, T> ruleConversion = (Function) conversion;
        return map( ruleConversion, filter( new Predicate<SchemaRule>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public boolean test( SchemaRule rule )
            {
                return ruleClass.isInstance( rule );
            }
        }, loadAllSchemaRules() ) );
    }

    public <R extends SchemaRule> Iterator<R> schemaRules( final Class<R> ruleClass )
    {
        @SuppressWarnings({"UnnecessaryLocalVariable", "unchecked"/*the predicate ensures that this cast is safe*/})
        Iterator<R> result = (Iterator)filter( new Predicate<SchemaRule>()
        {
            @Override
            public boolean test( SchemaRule rule )
            {
                return ruleClass.isInstance( rule );
            }
        }, loadAllSchemaRules() );
        return result;
    }

    public Iterator<SchemaRule> loadAllSchemaRules()
    {
        return new PrefetchingIterator<SchemaRule>()
        {
            private final long highestId = schemaStore.getHighestPossibleIdInUse();
            private long currentId = 1; /*record 0 contains the block size*/
            private final byte[] scratchData = newRecordBuffer();

            @Override
            protected SchemaRule fetchNextOrNull()
            {
                while ( currentId <= highestId )
                {
                    long id = currentId++;
                    DynamicRecord record = schemaStore.forceGetRecord( id );
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
            records = schemaStore.getRecords( id );
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

    public NodePropertyExistenceConstraintRule nodePropertyExistenceConstraint( int labelId, int propertyKeyId )
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        return nodeConstraintRule( NodePropertyExistenceConstraintRule.class, labelId, propertyKeyId );
    }

    public RelationshipPropertyExistenceConstraintRule relationshipPropertyExistenceConstraint( int typeId,
            int propertyKeyId ) throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        return relationshipConstraintRule( RelationshipPropertyExistenceConstraintRule.class, typeId, propertyKeyId );
    }

    public UniquePropertyConstraintRule uniquenessConstraint( int labelId, final int propertyKeyId )
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        return nodeConstraintRule( UniquePropertyConstraintRule.class, labelId, propertyKeyId );
    }

    private <Rule extends NodePropertyConstraintRule> Rule nodeConstraintRule( Class<Rule> type,
            final int labelId, final int propertyKeyId )
            throws SchemaRuleNotFoundException, DuplicateEntitySchemaRuleException
    {
        Iterator<Rule> rules = schemaRules( cast( type ), type, new Predicate<Rule>()
        {
            @Override
            public boolean test( Rule item )
            {
                return item.getLabel() == labelId &&
                       item.containsPropertyKeyId( propertyKeyId );
            }
        } );
        if ( !rules.hasNext() )
        {
            throw new EntitySchemaRuleNotFoundException( EntityType.NODE, labelId, propertyKeyId );
        }

        Rule rule = rules.next();

        if ( rules.hasNext() )
        {
            throw new DuplicateEntitySchemaRuleException( EntityType.NODE, labelId, propertyKeyId );
        }
        return rule;
    }

    private <Rule extends RelationshipPropertyConstraintRule> Rule relationshipConstraintRule( Class<Rule> type,
            final int relationshipTypeId, final int propertyKeyId )
            throws SchemaRuleNotFoundException, DuplicateEntitySchemaRuleException
    {
        Iterator<Rule> rules = schemaRules( cast( type ), type, new Predicate<Rule>()
        {
            @Override
            public boolean test( Rule item )
            {
                return item.getRelationshipType() == relationshipTypeId &&
                       item.containsPropertyKeyId( propertyKeyId );
            }
        } );
        if ( !rules.hasNext() )
        {
            throw new EntitySchemaRuleNotFoundException( EntityType.RELATIONSHIP, relationshipTypeId, propertyKeyId );
        }

        Rule rule = rules.next();

        if ( rules.hasNext() )
        {
            throw new DuplicateEntitySchemaRuleException( EntityType.RELATIONSHIP, relationshipTypeId, propertyKeyId );
        }
        return rule;
    }
}
