/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.api.Nested;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.neo4j.internal.schema.IndexType.fromPublicApi;

public class ConstraintTest
{
    abstract static class AbstractConstraintTest extends ConstraintTestBase<WriteTestSupport>
    {
        @Override
        public WriteTestSupport newTestSupport()
        {
            return new WriteTestSupport();
        }

        @Override
        protected LabelSchemaDescriptor labelSchemaDescriptor( int labelId, int... propertyIds )
        {
            return SchemaDescriptors.forLabel( labelId, propertyIds );
        }

        @Override
        protected ConstraintDescriptor uniqueConstraintDescriptor( int labelId, int... propertyIds )
        {
            return ConstraintDescriptorFactory.uniqueForLabel( fromPublicApi( indexType() ), labelId, propertyIds );
        }
    }

    @Nested
    class BTreeIndexTest extends AbstractConstraintTest
    {
        @Override
        protected IndexType indexType()
        {
            return IndexType.BTREE;
        }
    }

    @Nested
    class RangeIndexTest extends AbstractConstraintTest
    {
        @Override
        protected IndexType indexType()
        {
            return IndexType.RANGE;
        }

        @Override
        public WriteTestSupport newTestSupport()
        {
            return new WriteTestSupport()
            {
                @Override
                protected TestDatabaseManagementServiceBuilder configure( TestDatabaseManagementServiceBuilder builder )
                {
                    builder.setConfig( GraphDatabaseInternalSettings.range_indexes_enabled, true );
                    return super.configure( builder );
                }
            };
        }
    }
}
