package es.us.isa.ppinot.elasticsearch;

import es.us.isa.ppinot.model.MeasureDefinition;
import es.us.isa.ppinot.model.aggregated.AggregatedMeasure;
import es.us.isa.ppinot.model.derived.DerivedMultiInstanceMeasure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Measures
 * Copyright (C) 2016 Universidad de Sevilla
 *
 * @author resinas
 */
public class SingleInstanceMeasures {
    private Map<String, Object>  definitions = new HashMap<String, Object>();

    public Map<String, Object> getDefinitions() {
        return definitions;
    }

    public void addDefinitions(MeasureDefinition... definitions) {
        for (MeasureDefinition def : definitions) {
            addDefinition(def);
        }
    }

    public void addDefinition(MeasureDefinition definition) {
        definitions.put(definition.getId(), definitionOf(definition));
    }

    public List<MeasureDefinition> listDefinitions() {
        return listFromMap(definitions);
    }

    private List<MeasureDefinition> listFromMap(Map<String, Object> map) {
        List<MeasureDefinition> definitionList = new ArrayList<MeasureDefinition>();
        for (Object value : map.values()) {
            if (value instanceof MeasureDefinition) {
                definitionList.add((MeasureDefinition)value);
            } else {
                definitionList.addAll(listFromMap((Map<String, Object>) value));
            }
        }

        return definitionList;
    }

    private Object definitionOf(MeasureDefinition definition) {
        Object toAdd;
        if (definition instanceof AggregatedMeasure) {
            toAdd = aggregatedMeasureOf((AggregatedMeasure) definition);
        } else if (definition instanceof DerivedMultiInstanceMeasure) {
            toAdd = derivedMeasureOf((DerivedMultiInstanceMeasure) definition);
        } else {
            toAdd = instanceMeasureOf(definition);
        }
        return toAdd;
    }

    private Object derivedMeasureOf(DerivedMultiInstanceMeasure definition) {
        Map<String, Object> map = new HashMap<String, Object>();
        for (String key : definition.getUsedMeasureMap().keySet()) {
            map.put(key, definitionOf(definition.getUsedMeasureId(key)));
        }
        return map;
    }

    private Object instanceMeasureOf(MeasureDefinition definition) {
        return definition;
    }

    private Object aggregatedMeasureOf(AggregatedMeasure definition) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("from", instanceMeasureOf(definition.getBaseMeasure()));
        if (definition.getFilter() != null) {
            map.put("filter", instanceMeasureOf(definition.getFilter()));
        }
        if (definition.getPeriodReferencePoint() != null) {
            map.put("referencePoint", instanceMeasureOf(definition.getPeriodReferencePoint()));
        }
        if (definition.getGroupedBy() != null && !definition.getGroupedBy().isEmpty()) {
            int count = 1;
            for (MeasureDefinition def : definition.getGroupedBy()) {
                map.put("groupedBy-" + count, instanceMeasureOf(def));
                count++;
            }
        }

        return map;
    }
}
