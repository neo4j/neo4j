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
package org.neo4j.causalclustering.protocol.handshake;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.helpers.collection.Pair;

public class ClientHandshakeException extends Exception
{
    public ClientHandshakeException( String message )
    {
        super( message );
    }

    public ClientHandshakeException( String message, @Nullable Protocol.ApplicationProtocol negotiatedApplicationProtocol,
            List<Pair<String,Optional<Protocol.ModifierProtocol>>> negotiatedModifierProtocols )
    {
        super( message + " Negotiated application protocol: " + negotiatedApplicationProtocol +
                " Negotiated modifier protocols: " + negotiatedModifierProtocols );
    }
}
