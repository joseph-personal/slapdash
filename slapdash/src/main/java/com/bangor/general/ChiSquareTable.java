package com.bangor.general;

import com.bangor.exception.IntegerNotValidException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author joseph
 */
public class ChiSquareTable {

    /**
     * Constructor takes no params
     */
    public ChiSquareTable() {
    }

    public static List<Double> getP1() {
        List<Double> p1 = new ArrayList<Double>();
        p1.add(0.00016);
        p1.add(0.02010);
        p1.add(0.1148);
        p1.add(0.2971);
        p1.add(0.5543);
        p1.add(0.8720);
        p1.add(1.239);
        p1.add(1.646);
        p1.add(2.088);
        p1.add(2.559);
        p1.add(3.053);
        p1.add(3.571);
        p1.add(5.229);
        p1.add(8.260);
        p1.add(14.95);
        p1.add(29.71);
        return p1;
    }

    public static List<Double> getP5() {
        List<Double> p5 = new ArrayList<Double>();
        p5.add(0.00393);
        p5.add(0.1026);
        p5.add(0.3518);
        p5.add(0.7107);
        p5.add(1.1455);
        p5.add(1.635);
        p5.add(2.167);
        p5.add(2.733);
        p5.add(3.325);
        p5.add(3.940);
        p5.add(4.575);
        p5.add(5.226);
        p5.add(7.261);
        p5.add(10.85);
        p5.add(18.49);
        p5.add(34.76);
        return p5;
    }

    public static List<Double> getP25() {
        List<Double> p25 = new ArrayList<Double>();
        p25.add(0.1015);
        p25.add(0.5753);
        p25.add(1.213);
        p25.add(1.923);
        p25.add(2.675);
        p25.add(3.455);
        p25.add(4.255);
        p25.add(5.071);
        p25.add(5.899);
        p25.add(6.737);
        p25.add(7.584);
        p25.add(8.438);
        p25.add(11.04);
        p25.add(15.45);
        p25.add(24.48);
        p25.add(42.94);
        return p25;
    }

    public static List<Double> getP50() {
        List<Double> p50 = new ArrayList<Double>();
        p50.add(0.4549);
        p50.add(1.386);
        p50.add(2.366);
        p50.add(3.357);
        p50.add(4.351);
        p50.add(5.348);
        p50.add(6.346);
        p50.add(7.344);
        p50.add(8.343);
        p50.add(9.342);
        p50.add(10.34);
        p50.add(11.34);
        p50.add(14.34);
        p50.add(19.34);
        p50.add(29.34);
        p50.add(46.33);
        return p50;
    }

    public static List<Double> getP75() {
        List<Double> p75 = new ArrayList<Double>();
        p75.add(1.323);
        p75.add(2.773);
        p75.add(4.108);
        p75.add(5.385);
        p75.add(6.626);
        p75.add(7.841);
        p75.add(9.037);
        p75.add(10.22);
        p75.add(11.39);
        p75.add(12.55);
        p75.add(13.70);
        p75.add(14.84);
        p75.add(18.25);
        p75.add(23.83);
        p75.add(34.80);
        p75.add(56.33);
        return p75;
    }

    public static List<Double> getP95() {
        List<Double> p95 = new ArrayList<Double>();
        p95.add(3.841);
        p95.add(5.991);
        p95.add(7.815);
        p95.add(9.488);
        p95.add(11.07);
        p95.add(12.59);
        p95.add(14.07);
        p95.add(15.51);
        p95.add(16.92);
        p95.add(18.31);
        p95.add(19.68);
        p95.add(21.03);
        p95.add(25.00);
        p95.add(31.41);
        p95.add(43.77);
        p95.add(67.50);
        return p95;
    }

    public static List<Double> getP99() {
        List<Double> p99 = new ArrayList<Double>();
        p99.add(6.635);//1
        p99.add(9.210);//2
        p99.add(11.34);//3
        p99.add(13.28);//4
        p99.add(15.09);//5
        p99.add(16.81);//6
        p99.add(18.48);//7
        p99.add(20.09);//8
        p99.add(21.67);//9
        p99.add(23.21);//10
        p99.add(24.73);//11
        p99.add(26.22);//12
        p99.add(27.688);//13
        p99.add(29.141);//14
        p99.add(30.58);//15
        p99.add(32.00);//16
        p99.add(33.409);//17
        p99.add(34.805);//18
        p99.add(36.191);//19
        p99.add(37.57);//20
        p99.add(38.932);//21
        p99.add(40.289);//22
        p99.add(41.638);//23
        p99.add(42.980);//24
        p99.add(44.314);//25
        p99.add(45.642);//26
        p99.add(46.963);//27
        p99.add(48.278);//28
        p99.add(49.588);//29
        p99.add(50.89);//30
        p99.add(52.191);//31
        p99.add(53.486);//32
        p99.add(54.776);//33
        p99.add(56.061);//34
        p99.add(57.342);//35
        p99.add(58.619);//36
        p99.add(59.893);//37
        p99.add(61.162);//38
        p99.add(62.428);//39
        p99.add(63.691);//40
        p99.add(64.950);//41
        p99.add(66.206);//42
        p99.add(67.459);//43
        p99.add(68.710);//44
        p99.add(69.957);//45
        p99.add(71.201);//46
        p99.add(72.443);//47
        p99.add(73.683);//48
        p99.add(74.919);//49
        p99.add(76.15);//50
        return p99;
    }

//    public static double getP99Value(int v) throws IntegerNotValidException {
//        double double_p99Value;
//        switch (v) {
//            case 1:
//                double_p99Value = getP99().get(0);
//                break;
//            case 2:
//                double_p99Value = getP99().get(1);
//                break;
//            case 3:
//                double_p99Value = getP99().get(2);
//                break;
//            case 4:
//                double_p99Value = getP99().get(3);
//                break;
//            case 5:
//                double_p99Value = getP99().get(4);
//                break;
//            case 6:
//                double_p99Value = getP99().get(5);
//                break;
//            case 7:
//                double_p99Value = getP99().get(6);
//                break;
//            case 8:
//                double_p99Value = getP99().get(7);
//                break;
//            case 9:
//                double_p99Value = getP99().get(8);
//                break;
//            case 10:
//                double_p99Value = getP99().get(9);
//                break;
//            case 11:
//                double_p99Value = getP99().get(10);
//                break;
//            case 12:
//                double_p99Value = getP99().get(11);
//                break;
//            case 15:
//                double_p99Value = getP99().get(12);
//                break;
//            case 20:
//                double_p99Value = getP99().get(13);
//                break;
//            case 30:
//                double_p99Value = getP99().get(14);
//                break;
//            case 50:
//                double_p99Value = getP99().get(15);
//                break;
//            default: throw new IntegerNotValidException();
//        }
//
//        return double_p99Value;
//    }
}
