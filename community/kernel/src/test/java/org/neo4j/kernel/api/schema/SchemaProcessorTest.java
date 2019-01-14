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
package org.neo4j.kernel.api.schema;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;

import static org.hamcrest.MatcherAssert.assertThat;

public class SchemaProcessorTest
{
    private static final int LABEL_ID = 0;
    private static final int REL_TYPE_ID = 0;

    @Test
    public void shouldHandleCorrectDescriptorVersions()
    {
        List<String> callHistory = new ArrayList<>();
        SchemaProcessor processor = new SchemaProcessor()
        {
            @Override
            public void processSpecific( org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor schema )
            {
                callHistory.add( "LabelSchemaDescriptor" );
            }

            @Override
            public void processSpecific( org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor schema )
            {
                callHistory.add( "RelationTypeSchemaDescriptor" );
            }
        };

        disguisedLabel().processWith( processor );
        disguisedLabel().processWith( processor );
        disguisedRelType().processWith( processor );
        disguisedLabel().processWith( processor );
        disguisedRelType().processWith( processor );
        disguisedRelType().processWith( processor );

        assertThat( callHistory, Matchers.contains(
                "LabelSchemaDescriptor", "LabelSchemaDescriptor",
                "RelationTypeSchemaDescriptor", "LabelSchemaDescriptor",
                "RelationTypeSchemaDescriptor", "RelationTypeSchemaDescriptor" ) );
    }

    private SchemaDescriptor disguisedLabel()
    {
        return SchemaDescriptorFactory.forLabel( LABEL_ID, 1 );
    }

    private SchemaDescriptor disguisedRelType()
    {
        return SchemaDescriptorFactory.forRelType( REL_TYPE_ID, 1 );
    }
}
