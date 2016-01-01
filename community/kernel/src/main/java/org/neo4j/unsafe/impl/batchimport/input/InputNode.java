/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.helpers.Pair;

/**
 * Represents a node from an input source, for example a .csv file.
 */
public class InputNode extends InputEntity
{
    private final String[] labels;
    private final Long labelField;
    private final Object id;

    /**
     * @param id
     * @param properties
     * @param labels
     * @param labelField is a hack to bypass String[] labels, consumers should check that field first.
     */
    public InputNode( Object id, Object[] properties, Long firstPropertyId, String[] labels, Long labelField )
    {
        super( properties, firstPropertyId );
        this.id = id;
        this.labels = labels;
        this.labelField = labelField;
    }

    public Object id()
    {
        return id;
    }

    public String[] labels()
    {
        return labels;
    }

    public boolean hasLabelField()
    {
        return labelField != null;
    }

    public Long labelField()
    {
        return labelField;
    }

    @Override
    protected void toStringFields( Collection<Pair<String, ?>> fields )
    {
        super.toStringFields( fields );
        if ( hasLabelField() )
        {
            fields.add( Pair.of( "labelField", labelField ) );
        }
        else
        {
            fields.add( Pair.of( "labels", Arrays.toString( labels ) ) );
        }
    }
}
