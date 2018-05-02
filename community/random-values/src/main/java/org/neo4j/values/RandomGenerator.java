package org.neo4j.values;

import java.util.Random;

public class RandomGenerator implements Generator
{
    private final Random random;

    public RandomGenerator( Random random )
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
        return random.nextFloat();
    }

    @Override
    public double nextDouble()
    {
        return random.nextDouble();
    }
}
