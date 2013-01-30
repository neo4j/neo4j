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

import static java.util.Collections.unmodifiableCollection;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.helpers.collection.NestingIterable;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;

/**
 * A cache of {@link SchemaRule schema rules} as well as enforcement of schema consistency. 
 * Will always reflect the committed state of the schema store.
 * 
 * Assume synchronization/locking is done outside, with locks.
 * 
 * @author Mattias Persson
 * @author Stefan Plantikow
 */
public class SchemaCache
{
    private final Map<Long, Collection<SchemaRule>> rulesMap = new HashMap<Long, Collection<SchemaRule>>();
    private final Map<Long, SchemaRule> ruleByIdMap = new HashMap<Long, SchemaRule>();
    
    public SchemaCache( Iterable<SchemaRule> initialRules )
    {
        splitUpInitialRules( initialRules );
    }

    private void splitUpInitialRules( Iterable<SchemaRule> initialRules )
    {
        for ( SchemaRule rule : initialRules )
            addSchemaRule( rule );
    }

    private Collection<SchemaRule> getOrCreateSchemaRulesListForLabel( Long label )
    {
        Collection<SchemaRule> rulesForLabel = rulesMap.get( label );
        if ( rulesForLabel == null )
        {
            rulesForLabel = new HashSet<SchemaRule>();
            rulesMap.put( label, rulesForLabel );
        }
        return rulesForLabel;
    }
    
    public Iterable<SchemaRule> getSchemaRules()
    {
        return new NestingIterable<SchemaRule, Collection<SchemaRule>>( rulesMap.values() )
        {
            @Override
            protected Iterator<SchemaRule> createNestedIterator( Collection<SchemaRule> item )
            {
                return item.iterator();
            }
        };
    }
    
    public Collection<SchemaRule> getSchemaRules( long label )
    {
        Collection<SchemaRule> rulesForLabel = rulesMap.get( label );
        return rulesForLabel != null ? unmodifiableCollection( rulesForLabel ) : Collections.<SchemaRule>emptyList();
    }
    
    public void addSchemaRule( SchemaRule rule )
    {
        getOrCreateSchemaRulesListForLabel( rule.getLabel() ).add( rule );
        ruleByIdMap.put( rule.getId(), rule );
    }

    public void removeSchemaRule( long id )
    {
        SchemaRule rule = ruleByIdMap.remove( id );
        if ( rule == null )
            return;
        
        Collection<SchemaRule> rules = rulesMap.get( rule.getLabel() );
        if ( rules.remove( rules ) )
            if ( rules.isEmpty() )
                rulesMap.remove( rule.getLabel() );
    }
}
