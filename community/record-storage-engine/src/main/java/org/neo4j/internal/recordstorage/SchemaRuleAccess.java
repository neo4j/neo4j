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

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;

public interface SchemaRuleAccess
{
    static SchemaRuleAccess getSchemaRuleAccess( SchemaStore store, TokenHolders tokenHolders )
    {
        return new SchemaStorage( store, tokenHolders );
    }

    long newRuleId();

    Iterable<SchemaRule> getAll();

    SchemaRule loadSingleSchemaRule( long ruleId ) throws MalformedSchemaRuleException;

    Iterator<IndexDescriptor> indexesGetAll();

    /**
     * Find the IndexRule that matches the given {@link SchemaDescriptorSupplier}.
     *
     * @return an array of all the matching index rules. Empty array if none found.
     * @param index the target {@link IndexDescriptor}
     */
    IndexDescriptor[] indexGetForSchema( SchemaDescriptorSupplier index );

    /**
     * Find the IndexRule that has the given user supplied name.
     *
     * @param indexName the user supplied index name to look for.
     * @return the matching IndexRule, or null if no matching index rule was found.
     */
    IndexDescriptor indexGetForName( String indexName );

    /**
     * Get the constraint rule that matches the given ConstraintDescriptor
     * @param descriptor the ConstraintDescriptor to match
     * @return the matching ConstraintDescriptor
     * @throws SchemaRuleNotFoundException if no ConstraintDescriptor matches the given descriptor
     * @throws DuplicateSchemaRuleException if two or more ConstraintDescriptors match the given descriptor
     */
    ConstraintDescriptor constraintsGetSingle( ConstraintDescriptor descriptor ) throws SchemaRuleNotFoundException, DuplicateSchemaRuleException;

    Iterator<ConstraintDescriptor> constraintsGetAllIgnoreMalformed();

    SchemaRecordChangeTranslator getSchemaRecordChangeTranslator();

    /**
     * Write the given schema rule at the location given by its persistent id, overwriting any data that might be at that location already.
     * This is a non-transactional operation that is used during schema store migration.
     */
    void writeSchemaRule( SchemaRule rule ) throws KernelException;

    /**
     * Deletes the schema rule at the location given by the persistent id of the schema rule given as an argument.
     * This is a non-transactional operation that is primarily used for testing.
     */
    @VisibleForTesting
    void deleteSchemaRule( SchemaRule rule );
}
