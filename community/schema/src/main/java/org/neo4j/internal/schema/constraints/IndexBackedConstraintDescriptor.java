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
package org.neo4j.internal.schema.constraints;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.token.api.TokenIdPrettyPrinter;

public abstract class IndexBackedConstraintDescriptor extends AbstractConstraintDescriptor implements SchemaDescriptorSupplier
{
    private final LabelSchemaDescriptor schema;
    private final LabelSchemaDescriptor ownedIndexSchema;

    IndexBackedConstraintDescriptor( Type type, LabelSchemaDescriptor ownedIndexSchema )
    {
        super( type );
        this.schema = ownedIndexSchema;
        this.ownedIndexSchema = ownedIndexSchema;
    }

    @Override
    public LabelSchemaDescriptor schema()
    {
        return schema;
    }

    public LabelSchemaDescriptor ownedIndexSchema()
    {
        return ownedIndexSchema;
    }

    @Override
    public String prettyPrint( TokenNameLookup tokenNameLookup )
    {
        String labelName = escapeLabelOrRelTyp( tokenNameLookup.labelGetName( schema.getLabelId() ) );
        String nodeName = labelName.toLowerCase();

        return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT %s IS %s", nodeName, labelName,
                formatProperties( schema.getPropertyIds(), tokenNameLookup, nodeName ),
                constraintTypeText() );
    }

    protected abstract String constraintTypeText();

    protected String formatProperties( int[] propertyIds, TokenNameLookup tokenNameLookup, String nodeName )
    {
        return TokenIdPrettyPrinter.niceProperties( tokenNameLookup, propertyIds, nodeName + ".", propertyIds.length > 1 );
    }
}
