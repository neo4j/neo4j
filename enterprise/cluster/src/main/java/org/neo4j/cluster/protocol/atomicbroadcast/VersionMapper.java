/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

import java.util.HashMap;
import java.util.Map;

public class VersionMapper
{
    private static final Map<String, Long> classNameToSerialVersionUID = new HashMap<>();

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
