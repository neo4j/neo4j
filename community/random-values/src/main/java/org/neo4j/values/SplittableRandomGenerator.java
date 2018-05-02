package org.neo4j.values;

import java.util.SplittableRandom;

public class SplittableRandomGenerator implements Generator
{
    private final SplittableRandom random;

    public SplittableRandomGenerator( SplittableRandom random )
    {
        this.random = random;
    }

    @Override
    public long nextLong()
    {
        return random.nextLong();
    }

    @Override
    public boolean nextBoolean()
    {
        return random.nextBoolean();
    }

    @Override
    public int nextInt()
    {
        return random.nextInt();
    }

    @Override
    public int nextInt( int bound )
    {
        return random.nextInt( bound );
    }

    @Override
    public float nextFloat()
    {
        return (float) random.nextDouble();
    }

    @Override
    public double nextDouble()
    {
        return random.nextDouble();
    }
}
