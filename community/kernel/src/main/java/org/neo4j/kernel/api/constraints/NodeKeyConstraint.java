/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.constraints;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema_new.SchemaUtil;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;

/**
 * Description of uniqueness and existence constraint over nodes given a label and a property key id.
 */
public class NodeKeyConstraint extends NodePropertyConstraint
{
    public NodeKeyConstraint( LabelSchemaDescriptor descriptor )
    {
        super( descriptor );
    }

    public NewIndexDescriptor indexDescriptor()
    {
        return NewIndexDescriptorFactory.forSchema( descriptor );
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        String labelName = labelName( tokenNameLookup );
        String boundIdentifier = labelName.toLowerCase();
        String properties = SchemaUtil
                .niceProperties( tokenNameLookup, descriptor.getPropertyIds(), boundIdentifier + "." );
        if ( descriptor.isComposite() )
        {
            properties = "(" + properties + ")";
        }
        return String.format( "CONSTRAINT ON ( %s:%s ) ASSERT %s IS NODE KEY", boundIdentifier, labelName, properties );
    }

    @Override
    public String toString()
    {
        return userDescription( SchemaUtil.idTokenNameLookup );
    }
}
