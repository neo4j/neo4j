/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;

public class SchemaStorage
{
    private final SchemaStore schemaStore;

    public SchemaStorage( SchemaStore schemaStore )
    {
        this.schemaStore = schemaStore;
    }

    public IndexRule indexRule( long labelId, final long propertyKeyId ) throws SchemaRuleNotFoundException
    {
        Iterator<IndexRule> rules = schemaRules(
                IndexRule.class, labelId,
                new Predicate<IndexRule>()
                {
                    @Override
                    public boolean accept( IndexRule item )
                    {
                        return item.getPropertyKey() == propertyKeyId;
                    }
                } );

        if ( !rules.hasNext() )
        {
            throw new SchemaRuleNotFoundException( "Index rule for label:" + labelId + " and property:" +
                                                   propertyKeyId + " not found" );
        }

        IndexRule rule = rules.next();

        if ( rules.hasNext() )
        {
            throw new SchemaRuleNotFoundException( "Found more than one matching index" );
        }
        return rule;
    }

    public <T extends SchemaRule> Iterator<T> schemaRules( final Class<T> type, long labelId, Predicate<T> predicate )
    {
        return schemaRules( Functions.cast( type ), type, labelId, predicate );
    }

    public <R extends SchemaRule, T> Iterator<T> schemaRules(
            Function<? super R, T> conversion, final Class<R> ruleType,
            final long labelId, final Predicate<R> predicate )
    {
        @SuppressWarnings("unchecked"/*the predicate ensures that this is safe*/)
        Function<SchemaRule, T> ruleConversion = (Function) conversion;
        return map( ruleConversion, filter( new Predicate<SchemaRule>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public boolean accept( SchemaRule rule )
            {
                return rule.getLabel() == labelId &&
                       rule.getKind().getRuleClass() == ruleType &&
                       predicate.accept( (R) rule );
            }
        }, schemaStore.loadAll() ) );
    }

    public <R extends SchemaRule, T> Iterator<T> schemaRules(
            Function<? super R, T> conversion, final SchemaRule.Kind kind,
            final Predicate<R> predicate )
    {
        @SuppressWarnings("unchecked"/*the predicate ensures that this is safe*/)
        Function<SchemaRule, T> ruleConversion = (Function) conversion;
        return map( ruleConversion, filter( new Predicate<SchemaRule>()
        {
            @SuppressWarnings("unchecked")
            @Override
            public boolean accept( SchemaRule rule )
            {
                return rule.getKind() == kind &&
                       predicate.accept( (R) rule );
            }
        }, schemaStore.loadAll() ) );
    }

    public long newRuleId()
    {
        return schemaStore.nextId();
    }

    public UniquenessConstraintRule uniquenessConstraint( long labelId, final long propertyKeyId )
            throws SchemaRuleNotFoundException
    {
        Iterator<UniquenessConstraintRule> rules = schemaRules(
                UniquenessConstraintRule.class, labelId,
                new Predicate<UniquenessConstraintRule>()
                {
                    @Override
                    public boolean accept( UniquenessConstraintRule item )
                    {
                        return item.containsPropertyKeyId( propertyKeyId );
                    }
                } );
        if ( !rules.hasNext() )
        {
            throw new SchemaRuleNotFoundException( "Index rule for label:" + labelId + " and property:" +
                                                   propertyKeyId + " not found" );
        }

        UniquenessConstraintRule rule = rules.next();

        if ( rules.hasNext() )
        {
            throw new SchemaRuleNotFoundException( "Found more than one matching index" );
        }
        return rule;
    }
}
