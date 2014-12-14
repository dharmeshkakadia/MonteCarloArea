package org.packt.mesos;

import java.util.ArrayList;
import java.util.List;

public class Expression {
    List<Term> terms;

    public Expression(){
        terms=new ArrayList<Term>();
    }

    public Expression(List<Term> terms){
        this.terms=terms;
    }

    public boolean addTerm(Term term){
        return terms.add(term);
    }

    public double evaluate(double x){
        double value=0;
        for (Term term : terms) {
            value+=term.evaluate(x);
        }
        return value;
    }

    public static Expression fromString(String s){
        Expression expression=new Expression();
        String[] terms = s.split("\\+");
        for (String term : terms) {
            expression.addTerm(Term.fromString(term));
        }
        return expression;
    }

    @Override
    public String toString() {
        StringBuilder builder=new StringBuilder();
        int i;
        for (i=0; i<terms.size()-1; i++) {
            builder.append(terms.get(i)).append(" + ");
        }
        builder.append(terms.get(i));
        return builder.toString();
    }
}

class Term{
    double coefficient;
    double exponent;

    Term(){
        coefficient=exponent=0;
    }

    Term(double coefficient, double exponent){
        this.coefficient=coefficient;
        this.exponent=exponent;
    }

    public static Term fromString(String term){
        double coefficient=1;
        double exponent=0;
        String[] splits=term.split("x",-1);

        if(splits.length>0) {
            String coefficientString=splits[0].trim();
            if(!coefficientString.isEmpty()) {
                coefficient = Double.parseDouble(coefficientString);
            }
        }

        if (splits.length>1) {
            exponent=1;
            String exponentString = splits[1].trim();
            if (!exponentString.isEmpty()) {
                exponent = Double.parseDouble(exponentString);
            }
        }
        return new Term(coefficient, exponent);
    }
    @Override
    public String toString() {
        return coefficient+"x^"+exponent;
    }

    public double evaluate(double x){
        return coefficient*Math.pow(x,exponent);
    }
}