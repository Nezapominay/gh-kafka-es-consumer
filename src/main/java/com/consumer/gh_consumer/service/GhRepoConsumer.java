package com.consumer.gh_consumer.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class GhRepoConsumer {

    private static final Logger log = LoggerFactory.getLogger(GhRepoConsumer.class);

    private static final long LOG_EVERY_BATCHES = 20;
    private static final long LOG_EVERY_DOCS = 50_000;
    private static final long LOG_EVERY_CHUNKS = 200;

    private final ElasticsearchClient es;
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${app.elastic.index}")
    private String index;

    @Value("${app.elastic.bulk-chunk:500}")
    private int bulkChunk;

    private final AtomicLong totalBatches = new AtomicLong();
    private final AtomicLong totalMessages = new AtomicLong();
    private final AtomicLong totalIndexed = new AtomicLong();
    private final AtomicLong totalSkipped = new AtomicLong();
    private final AtomicLong totalBulkErrors = new AtomicLong();

    private final AtomicLong totalChunks = new AtomicLong();

    public GhRepoConsumer(ElasticsearchClient es) {
        this.es = es;
    }

    @KafkaListener(
            id = "ghRepoListener",
            idIsGroup = false,
            topics = "${app.kafka.topic}"
    )
    public void consume(List<String> payloads, Acknowledgment ack) {
        long batchNo = totalBatches.incrementAndGet();
        long startNs = System.nanoTime();

        int batchSize = (payloads == null ? 0 : payloads.size());
        totalMessages.addAndGet(batchSize);

        if (batchNo % LOG_EVERY_BATCHES == 0) {
            log.info("[BATCH #{}] received size={}", batchNo, batchSize);
        }

        if (payloads == null || payloads.isEmpty()) {
            ack.acknowledge();
            return;
        }

        try {
            int chunkSize = Math.max(1, bulkChunk);

            for (int from = 0; from < payloads.size(); from += chunkSize) {
                int to = Math.min(from + chunkSize, payloads.size());
                List<String> chunk = payloads.subList(from, to);

                BulkResult r = bulkIndexChunk(chunk);

                totalIndexed.addAndGet(r.indexed);
                totalSkipped.addAndGet(r.skipped);
                totalBulkErrors.addAndGet(r.bulkErrors);

                long chunkNo = totalChunks.incrementAndGet();
                if (chunkNo % LOG_EVERY_CHUNKS == 0) {
                    log.info("[CHUNK #{}] batch={} {}-{} => indexed={}, skipped={}, bulkErrors={} totals: msg={}, indexed={}, skipped={}, bulkErrors={}",
                            chunkNo, batchNo, from, to,
                            r.indexed, r.skipped, r.bulkErrors,
                            totalMessages.get(), totalIndexed.get(), totalSkipped.get(), totalBulkErrors.get());
                }
            }

            long indexedNow = totalIndexed.get();
            if (indexedNow % LOG_EVERY_DOCS < (long) batchSize) {
                log.info("[PROGRESS] indexed={} (messagesSeen={}, skipped={}, bulkErrors={}, batches={})",
                        indexedNow, totalMessages.get(), totalSkipped.get(), totalBulkErrors.get(), totalBatches.get());
            }

            long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

            if (batchNo % LOG_EVERY_BATCHES == 0) {
                log.info("[BATCH #{}] DONE took={}ms totals: batches={}, msg={}, indexed={}, skipped={}, bulkErrors={}",
                        batchNo, tookMs,
                        totalBatches.get(), totalMessages.get(), totalIndexed.get(),
                        totalSkipped.get(), totalBulkErrors.get());
            }

            ack.acknowledge();
        } catch (Exception e) {
            log.error("[BATCH #{}] FAILED - will NOT ack (batch will be retried)", batchNo, e);
        }
    }

    private record BulkResult(int indexed, int skipped, int bulkErrors) {}

    private BulkResult bulkIndexChunk(List<String> chunk) throws Exception {
        int skipped = 0;

        var docs = new java.util.ArrayList<Doc>(chunk.size());

        for (String payload : chunk) {
            if (payload == null || payload.isBlank()) {
                skipped++;
                continue;
            }
            try {
                JsonNode node = mapper.readTree(payload);
                String id = resolveId(node, payload);
                docs.add(new Doc(id, node));
            } catch (Exception ex) {
                skipped++;
            }
        }

        if (docs.isEmpty()) {
            return new BulkResult(0, skipped, 0);
        }

        BulkResponse resp = es.bulk(b -> {
            for (Doc d : docs) {
                b.operations(op -> op.index(i -> i
                        .index(index)
                        .id(d.id)
                        .document(d.node)   // ВАЖНО: document(...) вместо withJson(...)
                ));
            }
            return b;
        });

        int indexedOps = docs.size();

        int bulkErrors = 0;
        if (resp.errors()) {
            bulkErrors = (int) resp.items().stream().filter(it -> it.error() != null).count();

            resp.items().stream()
                    .filter(it -> it.error() != null)
                    .limit(5)
                    .forEach(it -> log.error("Bulk item error: type={}, reason={}",
                            it.error().type(), it.error().reason()));
        }

        return new BulkResult(indexedOps, skipped, bulkErrors);
    }

    private static class Doc {
        final String id;
        final JsonNode node;
        Doc(String id, JsonNode node) {
            this.id = id;
            this.node = node;
        }
    }

    private String resolveId(JsonNode node, String payload) {
        if (node != null) {
            if (node.hasNonNull("id")) return node.get("id").asText();
            if (node.hasNonNull("full_name")) return node.get("full_name").asText();
            if (node.hasNonNull("name")) return node.get("name").asText();
        }
        return Integer.toHexString(payload.hashCode());
    }
}