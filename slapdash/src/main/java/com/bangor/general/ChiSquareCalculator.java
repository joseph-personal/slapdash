/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bangor.general;

import com.bangor.exception.IntegerNotValidException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author joseph
 */
public class ChiSquareCalculator {
    
    private Map<String, List<Double>> map_categories;
    private int int_numOfObservations;

    /**
     * created for testing class
     * @param args 
     */
    public static void main(String[] args){
        
        List<Double> list_varsZero = new ArrayList<Double>();
        list_varsZero.add(0.0);
        list_varsZero.add(0.1);
        
        List<Double> list_varsOne = new ArrayList<Double>();
        list_varsOne.add(1.0);
        list_varsOne.add(0.1);
        
        
        List<Double> list_varsTwo = new ArrayList<Double>();
        list_varsTwo.add(2.0);
        list_varsTwo.add(0.1);
        
        List<Double> list_varsThree = new ArrayList<Double>();
        list_varsThree.add(3.0);
        list_varsThree.add(0.1);
        
        List<Double> list_varsFour = new ArrayList<Double>();
        list_varsFour.add(4.0);
        list_varsFour.add(0.1);
        
        List<Double> list_varsFive = new ArrayList<Double>();
        list_varsFive.add(5.0);
        list_varsFive.add(0.1);
        
        List<Double> list_varsSix = new ArrayList<Double>();
        list_varsSix.add(6.0);
        list_varsSix.add(0.1);
        
        List<Double> list_varsSeven = new ArrayList<Double>();
        list_varsSeven.add(7.0);
        list_varsSeven.add(0.1);
        
        List<Double> list_varsEight = new ArrayList<Double>();
        list_varsEight.add(8.0);
        list_varsEight.add(0.1);
        
        List<Double> list_varsNine = new ArrayList<Double>();
        list_varsNine.add(9.0);
        list_varsNine.add(0.1);
        
        Map<String, List<Double>> map_categories = new HashMap<String, List<Double>>();
        
        map_categories.put("0", list_varsZero);
        map_categories.put("1", list_varsOne);
        map_categories.put("2", list_varsTwo);
        map_categories.put("3", list_varsThree);
        map_categories.put("4", list_varsFour);
        map_categories.put("5", list_varsFive);
        map_categories.put("6", list_varsSix);
        map_categories.put("7", list_varsSeven);
        map_categories.put("8", list_varsEight);
        map_categories.put("9", list_varsNine);
        
        ChiSquareCalculator chiSquareCalc = new ChiSquareCalculator(map_categories);
        
        double inaccuracy = chiSquareCalc.getInaccuracy();
        boolean doesPass = false;
        try {
            doesPass = chiSquareCalc.doesPass(99, inaccuracy);
        } catch (IntegerNotValidException ex) {
            Logger.getLogger(ChiSquareCalculator.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println("inaccuracy: " + inaccuracy);
        System.out.println("doesPass = " + doesPass);
    }
    
    /**
     * 
     * @param map_categories
     * @param int_numOfObservations 
     */
    public ChiSquareCalculator(Map<String, List<Double>> map_categories, int int_numOfObservations){
        this.map_categories = map_categories;
        this.int_numOfObservations = int_numOfObservations;
    }
    
    /**
     * 
     * @param map_categories 
     */
    public ChiSquareCalculator(Map<String, List<Double>> map_categories){
        this.map_categories = map_categories;
        
        this.int_numOfObservations = getNumberOfObservations();
    }
    
    /**
     * sets the categories map for this chi-square execution
     * @param map_categories - of type {String mapName, List{double occurences, double probability(in decimal)} }
     */
    public void setMap_categories(Map<String, List<Double>> map_categories) {
        this.map_categories = map_categories;
    }

    public void setInt_numOfObservations(int int_numOfObservations) {
        this.int_numOfObservations = int_numOfObservations;
    }

    public Map getMap_categories() {
        return map_categories;
    }

    public int getInt_numOfObservations() {
        return int_numOfObservations;
    }
    
    private int getNumberOfObservations(){
        int int_numOfObservationsLocal = 0;
        for (Map.Entry pairs : this.map_categories.entrySet()) {
            
            double double_occurencies = (Double)((List)pairs.getValue()).get(0);
            
            int_numOfObservationsLocal += double_occurencies;
        }
        
        return int_numOfObservationsLocal;
    }
    
    /**
     * gets the inaccuracy of the observations in the map.
     * @return 
     */
    public double getInaccuracy(){
        double double_inaccuracy;
        
        double double_value = 0;
        for (Map.Entry pairs : this.map_categories.entrySet()) {
            
            double double_occurencies = (Double)((List)pairs.getValue()).get(0);
            double double_probOfThisCategory = (Double)((List)pairs.getValue()).get(1);
            
            double double_stepOne = (double_occurencies*double_occurencies) / double_probOfThisCategory;
            double double_stepTwo = double_stepOne - this.int_numOfObservations;
            
            double_value += (double_stepTwo);
        }
        
        double_inaccuracy = (1.0/this.int_numOfObservations) * (long)double_value;
        
        return double_inaccuracy;
    }
    
    /**
     * returns whether or not the values are likely to be random according to the chi-square table
     * @param p - the percentage point at which the user wishes the sequence to pass.
     * @return 
     */
    public boolean doesPass(int p, double inaccuracy) throws IntegerNotValidException{
        //TODO: WORK OUT HOW THIS TEST WORKS!!
        boolean pass = false;
        double double_passValue;
        switch (p){
            case 1: 
                double_passValue = ChiSquareTable.getP1().get(this.int_numOfObservations - 2);
                pass = (inaccuracy > double_passValue)? true : false;
                        break;
            case 5: 
                double_passValue = ChiSquareTable.getP5().get(this.int_numOfObservations - 2);
                pass = (inaccuracy > double_passValue)? true : false;
                         break;
            case 25: 
                double_passValue = ChiSquareTable.getP25().get(this.int_numOfObservations - 2);
                pass = (inaccuracy > double_passValue)? true : false;
                         break;
            case 50: 
                double_passValue = ChiSquareTable.getP50().get(this.int_numOfObservations - 2);
                pass = (inaccuracy < double_passValue)? true : false;
                         break;
            case 75: 
                double_passValue = ChiSquareTable.getP75().get(this.int_numOfObservations - 2);
                pass = (inaccuracy < double_passValue)? true : false;
                         break;
            case 95: 
                double_passValue = ChiSquareTable.getP95().get(this.int_numOfObservations - 2);
                pass = (inaccuracy < double_passValue)? true : false;
                         break;
            case 99: 
                double_passValue = ChiSquareTable.getP99().get(this.int_numOfObservations - 2);
                pass = (inaccuracy <= double_passValue)? true : false;
                         break;
            default: throw new IntegerNotValidException();
        }
        
        return pass;
    }
    
}
