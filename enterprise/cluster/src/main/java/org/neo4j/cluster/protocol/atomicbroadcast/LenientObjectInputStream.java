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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class LenientObjectInputStream extends ObjectInputStream
{
    private VersionMapper versionMapper;

    public LenientObjectInputStream( ByteArrayInputStream fis, VersionMapper versionMapper ) throws IOException
    {
        super( fis );
        this.versionMapper = versionMapper;
    }

    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException
    {
        ObjectStreamClass wireClassDescriptor = super.readClassDescriptor();
        if(!versionMapper.hasMappingFor( wireClassDescriptor.getName() )) {
            versionMapper.addMappingFor( wireClassDescriptor.getName(), wireClassDescriptor.getSerialVersionUID() );
        }

        Class localClass; // the class in the local JVM that this descriptor represents.
        try {
            localClass = Class.forName( wireClassDescriptor.getName() );
        } catch (ClassNotFoundException e) {
            return wireClassDescriptor;
        }
        ObjectStreamClass localClassDescriptor = ObjectStreamClass.lookup(localClass);
        if (localClassDescriptor != null) {
            final long localSUID = localClassDescriptor.getSerialVersionUID();
            final long wireSUID = wireClassDescriptor.getSerialVersionUID();
            if (wireSUID != localSUID) {
                wireClassDescriptor = localClassDescriptor;
            }
        }
        return wireClassDescriptor;
    }
}
