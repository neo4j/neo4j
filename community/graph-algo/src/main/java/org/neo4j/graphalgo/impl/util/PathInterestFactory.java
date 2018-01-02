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
package org.neo4j.graphalgo.impl.util;

import java.util.Comparator;

import org.neo4j.function.BiFunction;
import org.neo4j.kernel.impl.util.NoneStrictMath;

import static org.neo4j.graphalgo.impl.util.PathInterest.PriorityBasedPathInterest;
import static org.neo4j.graphalgo.impl.util.PathInterest.VisitCountBasedPathInterest;

/**
 * @author Anton Persson
 */
public class PathInterestFactory
{
    public static final Comparator<Comparable> STANDARD_COMPARATOR = new Comparator<Comparable>()
    {
        @Override
        public int compare( Comparable o1, Comparable o2 )
        {
            return o1.compareTo( o2 );
        }
    };

    public static PathInterest<? extends Comparable> single()
    {
        return SINGLE;
    }

    public static PathInterest<? extends Comparable> allShortest()
    {
        return ALL_SHORTEST;
    }

    public static PathInterest<? extends Comparable> all()
    {
        return ALL;
    }

    private static final PathInterest<? extends Comparable> SINGLE = new PathInterest<Comparable>()
    {
        @Override
        public Comparator<Comparable> comparator()
        {
            return STANDARD_COMPARATOR;
        }

        @Override
        public boolean canBeRuledOut( int numberOfVisits, Comparable pathPriority, Comparable oldPriority )
        {
            return numberOfVisits > 0 || pathPriority.compareTo( oldPriority ) >= 0;
        }

        @Override
        public boolean stillInteresting( int numberOfVisits )
        {
            return numberOfVisits <= 1;
        }

        @Override
        public boolean stopAfterLowestCost()
        {
            return true;
        }
    };

    private static final PathInterest<? extends Comparable> ALL_SHORTEST =
            new PriorityBasedPathInterest<Comparable>()
            {
                private BiFunction<Comparable,Comparable,Boolean> interestFunction;

                @Override
                public BiFunction<Comparable,Comparable,Boolean> interestFunction()
                {
                    if ( interestFunction == null )
                    {
                        interestFunction = new BiFunction<Comparable,Comparable,Boolean>()
                        {
                            @Override
                            public Boolean apply( Comparable newValue, Comparable oldValue )
                            {
                                return newValue.compareTo( oldValue ) <= 0;
                            }
                        };
                    }
                    return interestFunction;
                }

                @Override
                public Comparator<Comparable> comparator()
                {
                    return STANDARD_COMPARATOR;
                }
            };

    private static final PathInterest<? extends Comparable> ALL = new PathInterest<Comparable>()
    {
        @Override
        public Comparator<Comparable> comparator()
        {
            return STANDARD_COMPARATOR;
        }

        @Override
        public boolean canBeRuledOut( int numberOfVisits, Comparable pathPriority, Comparable oldPriority )
        {
            return false;
        }

        @Override
        public boolean stillInteresting( int numberOfVisits )
        {
            return true;
        }

        @Override
        public boolean stopAfterLowestCost()
        {
            return false;
        }
    };

    public static <P extends Comparable> PathInterest<P> numberOfShortest( final int numberOfWantedPaths )
    {
        if ( numberOfWantedPaths < 1 )
        {
            throw new IllegalArgumentException( "Can not create PathInterest with interested in less than 1 path." );
        }

        return new VisitCountBasedPathInterest<P>()
        {
            private Comparator<P> comparator = new Comparator<P>()
            {
                @Override
                public int compare( P o1, P o2 )
                {
                    return o1.compareTo( o2 );
                }
            };

            @Override
            int numberOfWantedPaths()
            {
                return numberOfWantedPaths;
            }

            @Override
            public Comparator<P> comparator()
            {
                return comparator;
            }
        };
    }

    public static PathInterest<Double> allShortest( double epsilon )
    {
        return new PriorityBasedTolerancePathInterest( epsilon );
    }

    public static PathInterest<Double> all( double epsilon )
    {
        return new AllTolerancePathInterest( epsilon );
    }

    public static PathInterest<Double> numberOfShortest( double epsilon, int numberOfWantedPaths )
    {
        return new VisitCountBasedTolerancePathInterest( epsilon, numberOfWantedPaths );
    }

    public static PathInterest<Double> single( double epsilon )
    {
        return new SingleTolerancePathInterest( epsilon );
    }

    private static class PriorityBasedTolerancePathInterest extends PriorityBasedPathInterest<Double>
    {
        private final double epsilon;
        private BiFunction<Double,Double,Boolean> interestFunction =
                new BiFunction<Double,Double,Boolean>()
                {
                    @Override
                    public Boolean apply( Double newValue, Double oldValue )
                    {
                        return NoneStrictMath.compare( newValue, oldValue, epsilon ) <= 0;
                    }
                };
        private final Comparator<Double> comparator;

        PriorityBasedTolerancePathInterest( final double epsilon )
        {
            this.epsilon = epsilon;
            this.comparator = new NoneStrictMath.CommonToleranceComparator( epsilon );
        }

        @Override
        public BiFunction<Double,Double,Boolean> interestFunction()
        {
            return interestFunction;
        }

        @Override
        public Comparator<Double> comparator()
        {
            return comparator;
        }
    }

    private static class VisitCountBasedTolerancePathInterest extends VisitCountBasedPathInterest<Double>
    {
        private final double epsilon;
        private final int numberOfWantedPaths;
        private final Comparator<Double> comparator;

        VisitCountBasedTolerancePathInterest( double epsilon, int numberOfWantedPaths )
        {
            this.epsilon = epsilon;
            this.numberOfWantedPaths = numberOfWantedPaths;
            this.comparator = new NoneStrictMath.CommonToleranceComparator( epsilon );
        }

        @Override
        int numberOfWantedPaths()
        {
            return numberOfWantedPaths;
        }

        @Override
        public Comparator<Double> comparator()
        {
            return comparator;
        }
    }

    private static class SingleTolerancePathInterest implements PathInterest<Double>
    {
        private final double epsilon;
        private final Comparator<Double> comparator;

        SingleTolerancePathInterest( double epsilon )
        {
            this.epsilon = epsilon;
            this.comparator = new NoneStrictMath.CommonToleranceComparator( epsilon );
        }

        @Override
        public Comparator<Double> comparator()
        {
            return comparator;
        }

        @Override
        public boolean canBeRuledOut( int numberOfVisits, Double pathPriority, Double oldPriority )
        {
            return numberOfVisits > 0 || NoneStrictMath.compare( pathPriority, oldPriority, epsilon ) >= 0;
        }

        @Override
        public boolean stillInteresting( int numberOfVisits )
        {
            return numberOfVisits <= 1;
        }

        @Override
        public boolean stopAfterLowestCost()
        {
            return true;
        }
    }

    private static class AllTolerancePathInterest implements PathInterest<Double>
    {
        private final Comparator<Double> comparator;

        AllTolerancePathInterest( double epsilon )
        {
            this.comparator = new NoneStrictMath.CommonToleranceComparator( epsilon );
        }

        @Override
        public Comparator<Double> comparator()
        {
            return comparator;
        }

        @Override
        public boolean canBeRuledOut( int numberOfVisits, Double pathPriority, Double oldPriority )
        {
            return false;
        }

        @Override
        public boolean stillInteresting( int numberOfVisits )
        {
            return true;
        }

        @Override
        public boolean stopAfterLowestCost()
        {
            return false;
        }
    }
}
