/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.SchemaReadWriteTestBase;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forRelType;

public class SchemaReadWriteTest extends SchemaReadWriteTestBase<EnterpriseWriteTestSupport>
{
    @Override
    public EnterpriseWriteTestSupport newTestSupport()
    {
        return new EnterpriseWriteTestSupport();
    }

    @Override
    protected LabelSchemaDescriptor labelDescriptor( int label, int... props )
    {
        return SchemaDescriptorFactory.forLabel( label, props );
    }

    @Override
    protected RelationTypeSchemaDescriptor typeDescriptor( int label, int...props )
    {
        return forRelType( label, props );
    }
}
