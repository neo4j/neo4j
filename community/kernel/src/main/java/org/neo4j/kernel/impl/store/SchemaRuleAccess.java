/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.StoreIndexDescriptor;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;

public interface SchemaRuleAccess
{
    @SuppressWarnings( "unchecked" )
    static SchemaRuleAccess getSchemaRuleAccess( RecordStore<?> store )
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

    SchemaRule loadSingleSchemaRule( long ruleId ) throws MalformedSchemaRuleException;

    Iterator<StoreIndexDescriptor> indexesGetAll();

    /**
     * Find the IndexRule that matches the given IndexDescriptor.
     *
     * @return  the matching IndexRule, or null if no matching IndexRule was found
     * @throws  IllegalStateException if more than one matching rule.
     * @param index the target {@link IndexReference}
     */
    StoreIndexDescriptor indexGetForSchema( IndexDescriptor index );

    /**
     * Get the constraint rule that matches the given ConstraintDescriptor
     * @param descriptor the ConstraintDescriptor to match
     * @return the matching ConstrainRule
     * @throws SchemaRuleNotFoundException if no ConstraintRule matches the given descriptor
     * @throws DuplicateSchemaRuleException if two or more ConstraintRules match the given descriptor
     */
    ConstraintRule constraintsGetSingle( ConstraintDescriptor descriptor )
    throws SchemaRuleNotFoundException, DuplicateSchemaRuleException;

    Iterator<ConstraintRule> constraintsGetAllIgnoreMalformed();
}
