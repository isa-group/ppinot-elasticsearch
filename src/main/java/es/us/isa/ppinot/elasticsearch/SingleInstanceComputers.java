package es.us.isa.ppinot.elasticsearch;

import es.us.isa.ppinot.evaluation.computers.MeasureComputer;
import es.us.isa.ppinot.evaluation.computers.MeasureComputerFactory;
import es.us.isa.ppinot.model.MeasureDefinition;
import es.us.isa.ppinot.model.ProcessInstanceFilter;
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
public class SingleInstanceComputers {
    private Map<String, Object> computers = new HashMap<String, Object>();
    private MeasureComputerFactory factory = new MeasureComputerFactory();
    private ProcessInstanceFilter filter;

    public SingleInstanceComputers(ProcessInstanceFilter filter) {
        this.filter = filter;
    }

    public Map<String, Object> getComputers() {
        return computers;
    }

    public void addDefinitions(MeasureDefinition... definitions) {
        for (MeasureDefinition def : definitions) {
            addDefinition(def);
        }
    }

    public void addDefinition(MeasureDefinition definition) {
        computers.put(definition.getId(), computerOf(definition));
    }

    public List<MeasureComputer> listComputers() {
        return listFromMap(computers);
    }

    private List<MeasureComputer> listFromMap(Map<String, Object> map) {
        List<MeasureComputer> definitionList = new ArrayList<MeasureComputer>();
        for (Object value : map.values()) {
            if (value instanceof MeasureComputer) {
                definitionList.add((MeasureComputer)value);
            } else {
                definitionList.addAll(listFromMap((Map<String, Object>) value));
            }
        }

        return definitionList;
    }

    private Object computerOf(MeasureDefinition definition) {
        Object toAdd;
        if (definition instanceof AggregatedMeasure) {
            toAdd = aggregatedMeasureOf((AggregatedMeasure) definition);
        } else if (definition instanceof DerivedMultiInstanceMeasure) {
            toAdd = derivedMeasureOf((DerivedMultiInstanceMeasure) definition);
        } else {
            toAdd = instanceComputerOf(definition);
        }
        return toAdd;
    }

    private Object instanceComputerOf(MeasureDefinition definition) {
        return factory.create(definition, filter);
    }

    private Object derivedMeasureOf(DerivedMultiInstanceMeasure definition) {
        Map<String, Object> map = new HashMap<String, Object>();
        for (String key : definition.getUsedMeasureMap().keySet()) {
            map.put(key, computerOf(definition.getUsedMeasureId(key)));
        }
        return map;
    }

    private Object aggregatedMeasureOf(AggregatedMeasure definition) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("from", instanceComputerOf(definition.getBaseMeasure()));
        if (definition.getFilter() != null) {
            map.put("filter", instanceComputerOf(definition.getFilter()));
        }
        if (definition.getPeriodReferencePoint() != null) {
            map.put("referencePoint", instanceComputerOf(definition.getPeriodReferencePoint()));
        }
        if (definition.getGroupedBy() != null && !definition.getGroupedBy().isEmpty()) {
            int count = 1;
            for (MeasureDefinition def : definition.getGroupedBy()) {
                map.put("groupedBy-" + count, instanceComputerOf(def));
                count++;
            }
        }

        return map;
    }
}
