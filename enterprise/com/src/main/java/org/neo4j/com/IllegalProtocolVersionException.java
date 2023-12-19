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
package org.neo4j.com;

/**
 * Thrown when a communication between client/server is attempted and either of internal protocol version
 * and application protocol doesn't match.
 *
 * @author Mattias Persson
 */
public class IllegalProtocolVersionException extends ComException
{
    private final byte expected;
    private final byte received;

    public IllegalProtocolVersionException( byte expected, byte received, String message )
    {
        super( message );
        this.expected = expected;
        this.received = received;
    }

    public byte getExpected()
    {
        return expected;
    }

    public byte getReceived()
    {
        return received;
    }

    @Override
    public String toString()
    {
        return "IllegalProtocolVersionException{expected=" + expected + ", received=" + received + "}";
    }
}
