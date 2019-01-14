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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.ConstraintTestBase;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;

public class ConstraintTest extends ConstraintTestBase<WriteTestSupport>
{
    @Override
    public WriteTestSupport newTestSupport()
    {
        return new WriteTestSupport();
    }

    @Override
    protected LabelSchemaDescriptor labelSchemaDescriptor( int labelId, int... propertyIds )
    {
        return SchemaDescriptorFactory.forLabel( labelId, propertyIds );
    }

    @Override
    protected ConstraintDescriptor uniqueConstraintDescriptor( int labelId, int... propertyIds )
    {
        return ConstraintDescriptorFactory.uniqueForLabel(  labelId, propertyIds  );
    }
}
