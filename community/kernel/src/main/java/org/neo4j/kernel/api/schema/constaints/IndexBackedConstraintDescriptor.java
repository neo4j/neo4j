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
package org.neo4j.kernel.api.schema.constaints;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.LabelSchemaSupplier;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;

public abstract class IndexBackedConstraintDescriptor extends ConstraintDescriptor implements LabelSchemaSupplier
{
    private final LabelSchemaDescriptor schema;

    IndexBackedConstraintDescriptor( Type type, LabelSchemaDescriptor schema )
    {
        super( type );
        this.schema = schema;
    }

    @Override
    public LabelSchemaDescriptor schema()
    {
        return schema;
    }

    public SchemaIndexDescriptor ownedIndexDescriptor()
    {
        return SchemaIndexDescriptorFactory.uniqueForSchema( schema );
    }

    @Override
    public String prettyPrint( TokenNameLookup tokenNameLookup )
    {
        String labelName = escapeLabelOrRelTyp( tokenNameLookup.labelGetName( schema.getLabelId() ) );
        String nodeName = labelName.toLowerCase();
        String properties = SchemaUtil.niceProperties( tokenNameLookup, schema.getPropertyIds(), nodeName + ".",
                schema().getPropertyIds().length > 1 );
        return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT %s IS %s", nodeName, labelName, properties,
                constraintTypeText() );
    }

    protected abstract String constraintTypeText();
}
