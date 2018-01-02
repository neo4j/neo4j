/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.neo4j.function.Consumer;

/**
 * A {@code TriggerInfo} contains the information about the events that are triggering a check point.
 *
 * The {@link org.neo4j.function.Consumer<String>#accept(String)} method can be used to enrich the description with
 * extra information. As an example, when the events triggering the check point are conditionalized wrt to a threshold,
 * thi can be used for adding the information about the threshold that actually allowed the check point to happen.
 */
public interface TriggerInfo extends Consumer<String>
{
    /**
     * This method can be used to retrieve the actual human-readable description about the events that triggered the
     * check point.
     *
     * @param transactionId the transaction id we are check pointing on
     * @return the description of the events that triggered check pointing
     */
    String describe( long transactionId );
}
