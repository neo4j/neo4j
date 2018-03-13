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
package org.neo4j.kernel.ha;

import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.RequestType;
import org.neo4j.com.TargetCaller;

public class HaRequestType implements RequestType
{
    private final TargetCaller targetCaller;
    private final ObjectSerializer objectSerializer;
    private final byte id;
    private final boolean unpack;

    public HaRequestType( TargetCaller targetCaller, ObjectSerializer objectSerializer, byte id, boolean unpack )
    {
        this.targetCaller = targetCaller;
        this.objectSerializer = objectSerializer;
        this.id = id;
        this.unpack = unpack;
    }

    @Override
    public TargetCaller getTargetCaller()
    {
        return targetCaller;
    }

    @Override
    public ObjectSerializer getObjectSerializer()
    {
        return objectSerializer;
    }

    @Override
    public byte id()
    {
        return id;
    }

    @Override
    public boolean responseShouldBeUnpacked()
    {
        return unpack;
    }
}
