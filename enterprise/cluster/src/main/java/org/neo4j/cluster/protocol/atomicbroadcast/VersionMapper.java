/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

import java.util.HashMap;
import java.util.Map;

public class VersionMapper
{
    private static final Map<String, Long> classNameToSerialVersionUID = new HashMap<String, Long>();

    public long mappingFor( String className )
    {
        return classNameToSerialVersionUID.get( className );
    }

    public boolean hasMappingFor( String className )
    {
        return classNameToSerialVersionUID.containsKey( className );
    }

    public void addMappingFor( String wireClassDescriptorName, long serialVersionUID )
    {
        classNameToSerialVersionUID.put(wireClassDescriptorName, serialVersionUID);
    }
}
