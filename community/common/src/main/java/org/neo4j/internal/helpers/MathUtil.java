/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.helpers;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import org.neo4j.util.Preconditions;

public final class MathUtil {
    public static final double DEFAULT_EPSILON = 1.0E-8;

    private static final long NON_DOUBLE_LONG = 0xFFE0_0000_0000_0000L; // doubles are exact integers up to 53 bits

    private MathUtil() {
        throw new AssertionError();
    }

    /**
     * Clamp the given value between the two given bounds.
     *
     * @param value  the value to clamp
     * @param lowerBound the lower boundary to clamp {@code value}
     * @param upperBound the upper boundary to clamp {@code value}
     * @return {@code value} if {@code value} is in between the bounds, otherwise the closest bound
     * @throws IllegalArgumentException when {@code lowerBound > upperBound}
     */
    public static int clamp(int value, int lowerBound, int upperBound) {
        Preconditions.checkArgument(
                lowerBound <= upperBound,
                "given lower bound, %d; is greater than given upper bound, %d.",
                lowerBound,
                upperBound);
        return Math.max(lowerBound, Math.min(value, upperBound));
    }

    /**
     * Clamp the given value between the two given bounds.
     *
     * @param value the value to clamp
     * @param lowerBound the lower boundary to clamp {@code value}
     * @param upperBound the upper boundary to clamp {@code value}
     * @return {@code value} if {@code value} is in between the bounds, otherwise the closest bound
     * @throws IllegalArgumentException when {@code lowerBound > upperBound}
     */
    public static long clamp(long value, long lowerBound, long upperBound) {
        Preconditions.checkArgument(
                lowerBound <= upperBound,
                "given lower bound, %d; is greater than given upper bound, %d.",
                lowerBound,
                upperBound);
        return Math.max(lowerBound, Math.min(value, upperBound));
    }

    /**
     * Clamp the given value between the two given bounds.
     *
     * @param value the value to clamp
     * @param lowerBound the lower boundary to clamp {@code value}
     * @param upperBound the upper boundary to clamp {@code value}
     * @return {@code value} if {@code value} is in between the bounds, otherwise the closest bound
     * @throws IllegalArgumentException when {@code lowerBound > upperBound}
     */
    public static float clamp(float value, float lowerBound, float upperBound) {
        Preconditions.checkArgument(
                lowerBound <= upperBound,
                "given lower bound, %g; is greater than given upper bound, %g.",
                lowerBound,
                upperBound);
        return Math.max(lowerBound, Math.min(value, upperBound));
    }

    /**
     * Clamp the given value between the two given bounds.
     *
     * @param value the value to clamp
     * @param lowerBound the lower boundary to clamp {@code value}
     * @param upperBound the upper boundary to clamp {@code value}
     * @return {@code value} if {@code value} is in between the bounds, otherwise the closest bound
     * @throws IllegalArgumentException when {@code lowerBound > upperBound}
     */
    public static double clamp(double value, double lowerBound, double upperBound) {
        Preconditions.checkArgument(
                lowerBound <= upperBound,
                "given lower bound, %g; is greater than given upper bound, %g.",
                lowerBound,
                upperBound);
        return Math.max(lowerBound, Math.min(value, upperBound));
    }

    /**
     * Calculates the portion of the first value to all values passed
     * @param n The values in the set
     * @return the ratio of n[0] to the sum all n, 0 if result is {@link Double#NaN}
     */
    public static double portion(double... n) {
        assert n.length > 0;

        double first = n[0];
        if (Math.abs(first) < DEFAULT_EPSILON) {
            return 0d;
        }
        double total = Arrays.stream(n).sum();
        return first / total;
    }

    // Tested by PropertyValueComparisonTest
    public static int compareDoubleAgainstLong(double lhs, long rhs) {
        if ((NON_DOUBLE_LONG & rhs) != NON_DOUBLE_LONG) {
            if (Double.isNaN(lhs)) {
                return +1;
            }
            if (Double.isInfinite(lhs)) {
                return lhs < 0 ? -1 : +1;
            }
            return BigDecimal.valueOf(lhs).compareTo(BigDecimal.valueOf(rhs));
        }
        return Double.compare(lhs, rhs);
    }

    public static int ceil(int dividend, int divisor) {
        return ((dividend - 1) / divisor) + 1;
    }

    public static long ceil(long dividend, long divisor) {
        return ((dividend - 1) / divisor) + 1;
    }

    /**
     * Compares two numbers given some amount of allowed error.
     */
    public static int compare(double x, double y, double eps) {
        return equals(x, y, eps) ? 0 : x < y ? -1 : 1;
    }

    /**
     * Returns true if both arguments are equal or within the range of allowed error (inclusive)
     */
    public static boolean equals(double x, double y, double eps) {
        return Math.abs(x - y) <= eps;
    }

    public static class CommonToleranceComparator implements Comparator<Double> {
        private final double epsilon;

        public CommonToleranceComparator(double epsilon) {
            this.epsilon = epsilon;
        }

        @Override
        public int compare(Double x, Double y) {
            return MathUtil.compare(x, y, epsilon);
        }
    }

    /**
     * Round a non-negative value up to the next multiple of a specified number.
     * @param value to round up.
     * @param multiplier
     * @return rounded up value.
     */
    public static long roundUp(long value, long multiplier) {
        return (value + multiplier - 1) / multiplier * multiplier;
    }

    /**
     * Ported from FdLibm to Java. See {@link StrictMath} for additional details
     * <pre>
     * ====================================================
     * Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
     *
     * Developed at SunSoft, a Sun Microsystems, Inc. business.
     * Permission to use, copy, modify, and distribute this
     * software is freely granted, provided that this notice
     * is preserved.
     * ====================================================
     * </pre>
     */
    public static final class Erf {
        // Coefficients for approximation to  erf on [0,0.84375]
        private static final double PP0 = 1.28379167095512558561e-01; // 0x3FC06EBA, 0x8214DB68
        private static final double PP1 = -3.25042107247001499370e-01; // 0xBFD4CD7D, 0x691CB913
        private static final double PP2 = -2.84817495755985104766e-02; // 0xBF9D2A51, 0xDBD7194F
        private static final double PP3 = -5.77027029648944159157e-03; // 0xBF77A291, 0x236668E4
        private static final double PP4 = -2.37630166566501626084e-05; // 0xBEF8EAD6, 0x120016AC
        private static final double QQ1 = 3.97917223959155352819e-01; // 0x3FD97779, 0xCDDADC09
        private static final double QQ2 = 6.50222499887672944485e-02; // 0x3FB0A54C, 0x5536CEBA
        private static final double QQ3 = 5.08130628187576562776e-03; // 0x3F74D022, 0xC4D36B0F
        private static final double QQ4 = 1.32494738004321644526e-04; // 0x3F215DC9, 0x221C1A10
        private static final double QQ5 = -3.96022827877536812320e-06; // 0xBED09C43, 0x42A26120

        // Coefficients for approximation to  erf  in [0.84375,1.25]
        private static final double PA0 = -2.36211856075265944077e-03; // 0xBF6359B8, 0xBEF77538
        private static final double PA1 = 4.14856118683748331666e-01; // 0x3FDA8D00, 0xAD92B34D
        private static final double PA2 = -3.72207876035701323847e-01; // 0xBFD7D240, 0xFBB8C3F1
        private static final double PA3 = 3.18346619901161753674e-01; // 0x3FD45FCA, 0x805120E4
        private static final double PA4 = -1.10894694282396677476e-01; // 0xBFBC6398, 0x3D3E28EC
        private static final double PA5 = 3.54783043256182359371e-02; // 0x3FA22A36, 0x599795EB
        private static final double PA6 = -2.16637559486879084300e-03; // 0xBF61BF38, 0x0A96073F
        private static final double QA1 = 1.06420880400844228286e-01; // 0x3FBB3E66, 0x18EEE323
        private static final double QA2 = 5.40397917702171048937e-01; // 0x3FE14AF0, 0x92EB6F33
        private static final double QA3 = 7.18286544141962662868e-02; // 0x3FB2635C, 0xD99FE9A7
        private static final double QA4 = 1.26171219808761642112e-01; // 0x3FC02660, 0xE763351F
        private static final double QA5 = 1.36370839120290507362e-02; // 0x3F8BEDC2, 0x6B51DD1C
        private static final double QA6 = 1.19844998467991074170e-02; // 0x3F888B54, 0x5735151D

        // Coefficients for approximation to  erfc in [1.25,1/0.35]
        private static final double RA0 = -9.86494403484714822705e-03; // 0xBF843412, 0x600D6435
        private static final double RA1 = -6.93858572707181764372e-01; // 0xBFE63416, 0xE4BA7360
        private static final double RA2 = -1.05586262253232909814e+01; // 0xC0251E04, 0x41B0E726
        private static final double RA3 = -6.23753324503260060396e+01; // 0xC04F300A, 0xE4CBA38D
        private static final double RA4 = -1.62396669462573470355e+02; // 0xC0644CB1, 0x84282266
        private static final double RA5 = -1.84605092906711035994e+02; // 0xC067135C, 0xEBCCABB2
        private static final double RA6 = -8.12874355063065934246e+01; // 0xC0545265, 0x57E4D2F2
        private static final double RA7 = -9.81432934416914548592e+00; // 0xC023A0EF, 0xC69AC25C
        private static final double SA1 = 1.96512716674392571292e+01; // 0x4033A6B9, 0xBD707687
        private static final double SA2 = 1.37657754143519042600e+02; // 0x4061350C, 0x526AE721
        private static final double SA3 = 4.34565877475229228821e+02; // 0x407B290D, 0xD58A1A71
        private static final double SA4 = 6.45387271733267880336e+02; // 0x40842B19, 0x21EC2868
        private static final double SA5 = 4.29008140027567833386e+02; // 0x407AD021, 0x57700314
        private static final double SA6 = 1.08635005541779435134e+02; // 0x405B28A3, 0xEE48AE2C
        private static final double SA7 = 6.57024977031928170135e+00; // 0x401A47EF, 0x8E484A93
        private static final double SA8 = -6.04244152148580987438e-02; // 0xBFAEEFF2, 0xEE749A62

        // Coefficients for approximation to  erfc in [1/.35,28]
        private static final double RB0 = -9.86494292470009928597e-03; // 0xBF843412, 0x39E86F4A
        private static final double RB1 = -7.99283237680523006574e-01; // 0xBFE993BA, 0x70C285DE
        private static final double RB2 = -1.77579549177547519889e+01; // 0xC031C209, 0x555F995A
        private static final double RB3 = -1.60636384855821916062e+02; // 0xC064145D, 0x43C5ED98
        private static final double RB4 = -6.37566443368389627722e+02; // 0xC083EC88, 0x1375F228
        private static final double RB5 = -1.02509513161107724954e+03; // 0xC0900461, 0x6A2E5992
        private static final double RB6 = -4.83519191608651397019e+02; // 0xC07E384E, 0x9BDC383F
        private static final double SB1 = 3.03380607434824582924e+01; // 0x403E568B, 0x261D5190
        private static final double SB2 = 3.25792512996573918826e+02; // 0x40745CAE, 0x221B9F0A
        private static final double SB3 = 1.53672958608443695994e+03; // 0x409802EB, 0x189D5118
        private static final double SB4 = 3.19985821950859553908e+03; // 0x40A8FFB7, 0x688C246A
        private static final double SB5 = 2.55305040643316442583e+03; // 0x40A3F219, 0xCEDF3BE6
        private static final double SB6 = 4.74528541206955367215e+02; // 0x407DA874, 0xE79FE763
        private static final double SB7 = -2.24409524465858183362e+01; // 0xC03670E2, 0x42712D62

        private static final double TINY = 1e-300;
        private static final double ONE = 1.00000000000000000000e+00; // 0x3FF00000, 0x00000000
        private static final double ERX = 8.45062911510467529297e-01; // 0x3FEB0AC1, 0x60000000
        private static final double EFX = 1.28379167095512586316e-01; // 0x3FC06EBA, 0x8214DB69
        private static final double EFX8 = 1.02703333676410069053e+00; // 0x3FF06EBA, 0x8214DB69

        private Erf() {}

        public static double erf(double x) {
            int hx = (int) (Double.doubleToRawLongBits(x) >> 32); // high word of x
            int ix = hx & 0x7fffffff; // high word of |x|
            if (ix >= 0x7ff00000) { // erf(nan) = nan
                int i = (hx >>> 31) << 1;
                return (double) (1 - i) + ONE / x; // erf(+-inf) = +-1
            }

            if (ix < 0x3feb0000) { // |x| < 0.84375
                if (ix < 0x3e300000) { // |x| < 2**-28
                    if (ix < 0x00800000) {
                        return 0.125 * (8.0 * x + EFX8 * x); // avoid underflow
                    }
                    return x + EFX * x;
                }
                double z = x * x;
                double r = PP0 + z * (PP1 + z * (PP2 + z * (PP3 + z * PP4)));
                double s = ONE + z * (QQ1 + z * (QQ2 + z * (QQ3 + z * (QQ4 + z * QQ5))));
                double y = r / s;
                return x + x * y;
            }
            if (ix < 0x3ff40000) { // 0.84375 <= |x| < 1.25
                double s = Math.abs(x) - ONE;
                double P = PA0 + s * (PA1 + s * (PA2 + s * (PA3 + s * (PA4 + s * (PA5 + s * PA6)))));
                double Q = ONE + s * (QA1 + s * (QA2 + s * (QA3 + s * (QA4 + s * (QA5 + s * QA6)))));
                if (hx >= 0) {
                    return ERX + P / Q;
                }
                return -ERX - P / Q;
            }
            if (ix >= 0x40180000) { // inf > |x| >= 6
                if (hx >= 0) {
                    return ONE - TINY;
                }
                return TINY - ONE;
            }
            x = Math.abs(x);
            double s = ONE / (x * x);
            double R;
            double S;
            if (ix < 0x4006DB6E) { // |x| < 1/0.35
                R = RA0 + s * (RA1 + s * (RA2 + s * (RA3 + s * (RA4 + s * (RA5 + s * (RA6 + s * RA7))))));
                S = ONE + s * (SA1 + s * (SA2 + s * (SA3 + s * (SA4 + s * (SA5 + s * (SA6 + s * (SA7 + s * SA8)))))));
            } else { // |x| >= 1/0.35
                R = RB0 + s * (RB1 + s * (RB2 + s * (RB3 + s * (RB4 + s * (RB5 + s * RB6)))));
                S = ONE + s * (SB1 + s * (SB2 + s * (SB3 + s * (SB4 + s * (SB5 + s * (SB6 + s * SB7))))));
            }

            double z = Double.longBitsToDouble(Double.doubleToRawLongBits(x) & 0x00000000FFFFFFFFL);
            double r = Math.exp(-z * z - 0.5625) * Math.exp((z - x) * (z + x) + R / S);
            if (hx >= 0) {
                return ONE - r / x;
            }
            return r / x - ONE;
        }
    }
}
