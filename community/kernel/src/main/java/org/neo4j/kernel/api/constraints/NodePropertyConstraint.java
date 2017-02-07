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
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.schema_new.LabelSchemaDescriptor;

/**
 * Base class describing property constraint on nodes.
 */
public abstract class NodePropertyConstraint implements PropertyConstraint
{
    protected final NodePropertyDescriptor descriptor;

    public NodePropertyConstraint( NodePropertyDescriptor descriptor )
    {
        this.descriptor = descriptor;
    }

    public final int label()
    {
        return descriptor.getLabelId();
    }

    public NodePropertyDescriptor descriptor()
    {
        return descriptor;
    }

    public boolean matches( NodePropertyDescriptor descriptor )
    {
        return this.descriptor.equals( descriptor );
    }

    public boolean matches( LabelSchemaDescriptor other )
    {
        return other != null &&
                descriptor.getLabelId() == other.getLabelId() &&
                descriptor.getPropertyKeyId() == other.getPropertyIds()[0];
        // this is safe because we are replacing this class before introducing composite constraints
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        NodePropertyConstraint that = (NodePropertyConstraint) o;
        return this.descriptor.equals( that.descriptor );

    }

    protected String labelName( TokenNameLookup tokenNameLookup)
    {
        String labelName = tokenNameLookup.labelGetName( descriptor.getLabelId() );
        //if the labelName contains a `:` we must escape it to avoid disambiguation,
        //e.g. CONSTRAINT on foo:bar:foo:bar
        if (labelName.contains( ":" )) {
            return "`" + labelName + "`";
        }
        else
        {
            return labelName;
        }
    }

    @Override
    public int hashCode()
    {
        return descriptor.hashCode();
    }
}
