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
package org.neo4j.internal.recordstorage;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptorSupplier;
import org.neo4j.util.VisibleForTesting;

public interface SchemaRuleAccess
{
    @SuppressWarnings( "unchecked" )
    static SchemaRuleAccess getSchemaRuleAccess( RecordStore<?> store,
            @SuppressWarnings( "unused" ) TokenHolders tokenHolders /* we'll need this for a future schema store refactoring */ )
    {
        AbstractBaseRecord record = store.newRecord();
        if ( record instanceof DynamicRecord )
        {
            RecordStore<DynamicRecord> schemaStore = (RecordStore<DynamicRecord>) store;
            return new SchemaStorage( schemaStore );
        }
        throw new IllegalArgumentException( "Cannot create SchemaRuleAccess for schema store: " + store );
    }

    long newRuleId();

    Iterable<SchemaRule> getAll();

    SchemaRule loadSingleSchemaRule( long ruleId ) throws MalformedSchemaRuleException;

    Iterator<StoreIndexDescriptor> indexesGetAll();

    /**
     * Find the IndexRule that matches the given {@link SchemaDescriptorSupplier}.
     *
     * @return  the matching IndexRule, or null if no matching IndexRule was found
     * @throws  IllegalStateException if more than one matching rule.
     * @param index the target {@link IndexReference}
     */
    StoreIndexDescriptor indexGetForSchema( SchemaDescriptorSupplier index );

    /**
     * Find the IndexRule that has the given user supplied name.
     *
     * @param indexName the user supplied index name to look for.
     * @return the matching IndexRule, or null if no matching index rule was found.
     */
    StoreIndexDescriptor indexGetForName( String indexName );

    /**
     * Get the constraint rule that matches the given ConstraintDescriptor
     * @param descriptor the ConstraintDescriptor to match
     * @return the matching ConstrainRule
     * @throws SchemaRuleNotFoundException if no ConstraintRule matches the given descriptor
     * @throws DuplicateSchemaRuleException if two or more ConstraintRules match the given descriptor
     */
    ConstraintRule constraintsGetSingle( ConstraintDescriptor descriptor ) throws SchemaRuleNotFoundException, DuplicateSchemaRuleException;

    Iterator<ConstraintRule> constraintsGetAllIgnoreMalformed();

    SchemaRecordChangeTranslator getSchemaRecordChangeTranslator();

    /**
     * Write the given schema rule at the location given by its persistent id, overwriting any data that might be at that location already.
     * This is a non-transactional operation that is used during schema store migration.
     */
    void writeSchemaRule( SchemaRule rule );

    /**
     * Deletes the schema rule at the location given by the persistent id of the schema rule given as an argument.
     * This is a non-transactional operation that is primarily used for testing.
     */
    @VisibleForTesting
    void deleteSchemaRule( SchemaRule rule );
}
