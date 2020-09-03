/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.block.procedure.primitive.IntObjectProcedure;
import org.eclipse.collections.api.map.primitive.IntObjectMap;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.values.storable.Value;

public abstract class PropertyBasedSchemaRecordChangeTranslator implements SchemaRecordChangeTranslator
{
    @Override
    public void createSchemaRule( TransactionRecordState recordState, SchemaRule rule ) throws KernelException
    {
        IntObjectMap<Value> properties = asMap( rule );
        long ruleId = rule.getId();
        recordState.schemaRuleCreate( ruleId, rule instanceof ConstraintDescriptor, rule );
        properties.forEachKeyValue( ( propertyKeyId, value ) -> recordState.schemaRuleSetProperty( ruleId, propertyKeyId, value, rule ) );
    }

    @Override
    public void dropSchemaRule( TransactionRecordState recordState, SchemaRule rule )
    {
        recordState.schemaRuleDelete( rule.getId(), rule );
    }

    @Override
    public void setConstraintIndexOwner( TransactionRecordState recordState, IndexDescriptor indexRule, long constraintId ) throws KernelException
    {
        setConstraintIndexOwnerProperty( constraintId,
                ( propertyKeyId, value ) -> recordState.schemaRuleSetIndexOwner( indexRule, constraintId, propertyKeyId, value ) );
    }

    protected abstract IntObjectMap<Value> asMap( SchemaRule rule ) throws KernelException;

    protected abstract void setConstraintIndexOwnerProperty( long constraintId, IntObjectProcedure<Value> proc ) throws KernelException;
}
