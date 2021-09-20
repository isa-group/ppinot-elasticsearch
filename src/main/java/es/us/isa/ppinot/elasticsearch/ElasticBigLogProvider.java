package es.us.isa.ppinot.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import es.us.isa.ppinot.evaluation.evaluators.BigLogProvider;
import es.us.isa.ppinot.evaluation.evaluators.LogInstance;
import es.us.isa.ppinot.evaluation.logs.AbstractLogProvider;
import es.us.isa.ppinot.evaluation.logs.EndMatcher;
import es.us.isa.ppinot.evaluation.logs.LogEntry;
import es.us.isa.ppinot.evaluation.logs.LogListener;
import es.us.isa.ppinot.evaluation.logs.LogProvider;

/**
 * ElasticBigLogProvider
 * Copyright (C) 2016 Universidad de Sevilla
 *
 * @author resinas
 */
public class ElasticBigLogProvider implements BigLogProvider {
    private static final Logger log = Logger.getLogger(ElasticBigLogProvider.class.getName());

    private TransportClient client;
    private String indexName;
    private final org.codehaus.jackson.map.ObjectMapper mapper;
    private final org.codehaus.jackson.map.ObjectReader reader;


    public ElasticBigLogProvider(TransportClient client, String indexName) {
        this.client = client;
        this.indexName = indexName;
        this.mapper = new org.codehaus.jackson.map.ObjectMapper();
        this.reader = this.mapper.reader(LogInstance.class);
    }

    public void loadLog(LogProvider logProvider, EndMatcher endMatcher) {
        DB db = DBMaker.tempFileDB().fileMmapEnable().concurrencyDisable().allocateIncrement(256*1024*1024).fileDeleteAfterClose().make();

        final Map<String, byte[]> map = db.hashMap("instances")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen();

        final BulkProcessor bulkProcessor = createBulkProcessor();

        final org.codehaus.jackson.map.ObjectReader reader = mapper.reader(mapper.getTypeFactory().constructCollectionType(List.class, LogEntry.class));

        try {
            log.info("Processing...: ");
            DateTime start = DateTime.now();

            logProvider.registerListener(new LogListener() {
                public void update(LogEntry entry)
                {
                    if (map.containsKey(entry.getInstanceId())) {
                        byte[] entriesJson = map.get(entry.getInstanceId());
                        try {
                            List<LogEntry> entries = reader.readValue(entriesJson);
                            entries.add(entry);
                            entriesJson = mapper.writeValueAsBytes(entries);
                        } catch (IOException e) {
                            log.log(Level.WARNING, "Error reading logProvider", e);
                        }
                        map.put(entry.getInstanceId(), entriesJson);
                    } else {
                        List<LogEntry> entries = new ArrayList<LogEntry>();
                        entries.add(entry);
                        try {
                            map.put(entry.getInstanceId(), mapper.writeValueAsBytes(entries));
                        } catch (IOException e) {
                            log.log(Level.WARNING, "Error reading logProvider", e);
                        }
                    }
                }
            });

            logProvider.processLog();

            log.info("Finished processing (Took "+ new Duration(start, DateTime.now()).getMillis() + " ms)" );



            for (String id : map.keySet()) {
                List<LogEntry> entries = reader.readValue(map.get(id));
                LogInstance instance = new LogInstance();
                instance.setInstanceId(id);
                Collections.sort(entries, new Comparator<LogEntry>() {
                    public int compare(LogEntry o1, LogEntry o2) {
                        return o1.getTimeStamp().compareTo(o2.getTimeStamp());
                    }
                });

                instance.setEntries(entries);
                instance.setStart(entries.get(0).getTimeStamp());
                LogEntry last = entries.get(entries.size() - 1);
                if (endMatcher.matches(last)) {
                    instance.setEnd(last.getTimeStamp());
                }

                bulkProcessor.add(new IndexRequest(indexName, "instance", id).source(mapper.writeValueAsBytes(instance)));

            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        bulkProcessor.close();

        db.close();
    }

    public void preprocessLog() {

    }

    public LogProvider create(Interval i, IntervalCondition condition) {
        if (IntervalCondition.START.equals(condition)) {
            return new ElasticLogProvider(QueryBuilders.rangeQuery("start").from(i.getStart()).to(i.getEnd()), i);
        } else if (IntervalCondition.ACTIVE.equals(condition)) {
            return new ElasticLogProvider(QueryBuilders.boolQuery()
                    .must(QueryBuilders.rangeQuery("start").to(i.getEnd()))
                    .must(QueryBuilders.rangeQuery("end").from(i.getStart())), i);
        } else  {
            return new ElasticLogProvider(QueryBuilders.rangeQuery("end").from(i.getStart()).to(i.getEnd()), i);
        }
    }

    public void close() {

    }

    private class ElasticLogProvider extends AbstractLogProvider {
        private SearchResponse response;
        private Interval interval;

        public ElasticLogProvider(QueryBuilder queryBuilder, Interval interval) {
            this.interval = interval;
            response = client.prepareSearch(indexName)
                    .setPostFilter(queryBuilder)
                    .setScroll(new TimeValue(60000))
                    .setSize(100)
                    .execute().actionGet();

            log.info("Found " + response.getHits().getTotalHits() + " instances in " + response.getTook());
        }

        public void processLog() {
            DateTime endOfInterval = interval.getEnd().plus(1);
            Iterator<SearchHit> iterator = response.getHits().iterator();
            while (iterator.hasNext()) {
                while (iterator.hasNext()) {
                    SearchHit hit = iterator.next();
                    try {
                        LogInstance instance = reader.readValue(hit.source());
                        if (instance != null) {
                            for (LogEntry entry : instance.getEntries()) {
                                if (entry.getTimeStamp().isBefore(endOfInterval)) {
                                    updateListeners(entry);
                                } else {
                                    break;
                                }
                            }
                        }
                    } catch (IOException e) {
                        log.log(Level.WARNING, "Error processing JSON from ES", e);
                    }
                }

                response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
                iterator = response.getHits().iterator();
            }

        }
    }


    private BulkProcessor createBulkProcessor() {
        return BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    public void beforeBulk(long l, BulkRequest bulkRequest) {
                        log.info("Before " + l);
                    }

                    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                        log.info("After " + l + ": " + bulkResponse.hasFailures() + "("+bulkResponse.getTook()+")");
                        if (bulkResponse.hasFailures()) {
                            log.warning("Details "+l+": " + bulkResponse.buildFailureMessage());
                        }
                    }

                    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                        log.log(Level.WARNING, "After " + l,  throwable);
                    }
                }
        )
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }

}
