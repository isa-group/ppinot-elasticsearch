package es.us.isa.ppinot.elasticsearch;

import es.us.isa.ppinot.evaluation.evaluators.LogInstance;

import java.util.Map;

/**
 * MeasuredLogInstance
 * Copyright (C) 2016 Universidad de Sevilla
 *
 * @author resinas
 */
public class MeasuredLogInstance extends LogInstance {
    private Map<String, SingleInstanceMeasures> measures;

    public Map<String, SingleInstanceMeasures> getMeasures() {
        return measures;
    }

    public void setMeasures(Map<String, SingleInstanceMeasures> measures) {
        this.measures = measures;
    }
}
