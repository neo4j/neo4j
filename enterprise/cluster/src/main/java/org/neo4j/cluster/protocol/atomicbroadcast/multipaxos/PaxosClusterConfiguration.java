/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration of Paxos. This lists all the cluster instances and their Paxos roles.
 */
public class PaxosClusterConfiguration
{
    List<String> proposers = new ArrayList<String>();
    List<String> acceptors = new ArrayList<String>();
    List<String> learners = new ArrayList<String>();
    String coordinator;
    int allowedFailures = 1; // Number of allowed failures

    public PaxosClusterConfiguration(List<String> proposers, List<String> acceptors, List<String> learners, String coordinator, int allowedFailures)
    {
        this.proposers = proposers;
        this.acceptors = acceptors;
        this.learners = learners;
        this.coordinator = coordinator;
        this.allowedFailures = allowedFailures;
    }

    public List<String> getProposers()
    {
        return proposers;
    }

    public List<String> getAcceptors()
    {
        return acceptors;
    }

    public List<String> getLearners()
    {
        return learners;
    }

    public String getCoordinator()
    {
        return coordinator;
    }

    public int getAllowedFailures()
    {
        return allowedFailures;
    }
}
