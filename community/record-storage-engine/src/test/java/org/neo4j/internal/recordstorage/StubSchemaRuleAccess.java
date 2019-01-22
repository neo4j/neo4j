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

import java.util.Collections;
import java.util.Iterator;

import org.neo4j.kernel.impl.index.schema.ConstraintRule;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptorSupplier;

public class StubSchemaRuleAccess implements SchemaRuleAccess
{
    @Override
    public long newRuleId()
    {
        return 0;
    }

    @Override
    public Iterable<SchemaRule> getAll()
    {
        return Collections.emptyList();
    }

    @Override
    public SchemaRule loadSingleSchemaRule( long ruleId )
    {
        return null;
    }

    @Override
    public Iterator<StoreIndexDescriptor> indexesGetAll()
    {
        return null;
    }

    @Override
    public StoreIndexDescriptor indexGetForSchema( SchemaDescriptorSupplier supplier )
    {
        return null;
    }

    @Override
    public StoreIndexDescriptor indexGetForName( String indexName )
    {
        return null;
    }

    @Override
    public ConstraintRule constraintsGetSingle( ConstraintDescriptor descriptor )
    {
        return null;
    }

    @Override
    public Iterator<ConstraintRule> constraintsGetAllIgnoreMalformed()
    {
        return null;
    }

    @Override
    public SchemaRecordChangeTranslator getSchemaRecordChangeTranslator()
    {
        return null;
    }

    @Override
    public void writeSchemaRule( SchemaRule rule )
    {
    }

    @Override
    public void deleteSchemaRule( SchemaRule rule )
    {
    }
}
