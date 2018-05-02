package org.neo4j.values;

public interface Generator
{
    long nextLong();

    boolean nextBoolean();

    int nextInt();

    int nextInt( int bound );

    float nextFloat();

    double nextDouble();
}
