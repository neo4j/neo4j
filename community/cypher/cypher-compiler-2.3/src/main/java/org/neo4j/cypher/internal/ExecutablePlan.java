package org.neo4j.cypher.internal;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;

public interface ExecutablePlan
{
    void accept( Visitor visitor, Statement statement ) throws KernelException;
}
