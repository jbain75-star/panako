// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2026 AudioScout
package be.panako.strategy.panako.storage;

import be.panako.util.Config;
import be.panako.util.Key;
import java.sql.*;
import java.nio.file.*;
import java.util.*;

/** Standalone real-PostgreSQL check; no audio downloads or test-framework dependencies. */
public final class CompactStorageCheck {
    static void require(boolean ok, String message) { if (!ok) throw new AssertionError(message); }
    public static void main(String[] args) throws Exception {
        String url = System.getenv("PANAKO_PG_URL");
        require(url != null && url.matches("jdbc:postgresql://127\\.0\\.0\\.1:[0-9]+/ear_storage_benchmark_engine_[a-z0-9_]+"), "Use an isolated local engine test DB");
        if (args.length == 1 && args[0].equals("--expect-disabled-refusal")) {
            require("false".equalsIgnoreCase(System.getenv("PANAKO_PG_COMPACT")), "Disable compact mode for refusal check");
            boolean refused = false;
            try { new PanakoStoragePostgres(); } catch (IllegalStateException expected) { refused = true; }
            require(refused, "Packed data must not be silently invisible when compact mode is off");
            System.out.println("DISABLED COMPACT MODE REFUSAL: PASS");
            return;
        }
        require("true".equalsIgnoreCase(System.getenv("PANAKO_PG_COMPACT")), "Enable compact mode");
        Config.set(Key.PANAKO_PG_MAX_HITS_PER_HASH, "2");
        // Constructor creates the ordinary schema then refuses missing compact SQL.
        boolean refused = false;
        try { new PanakoStoragePostgres(); } catch (IllegalStateException expected) { refused = true; }
        require(refused, "Missing schema must fail closed");
        try (Connection c=DriverManager.getConnection(url,System.getenv("PANAKO_PG_USER"),""); Statement st=c.createStatement()) {
            st.execute(Files.readString(Path.of(args[0])));
            PanakoStoragePostgres store = new PanakoStoragePostgres();
            store.storeMetadata(42, "example.wav", 30, 5);
            for (int r=40;r<45;r++) store.addToStoreQueue(100,r,r*2,4);
            store.processStoreQueue();
            st.execute("SELECT panako_compact_partition('panako_fingerprint_p000')");
            // Reprinting an already packed tuple must not consume an extra cap slot.
            store.addToStoreQueue(100,40,80,4); store.processStoreQueue();
            Map<Long,List<PanakoHit>> hits = new HashMap<>();
            store.addToQueryQueue(100); store.processQueryQueue(hits,0,Set.of(40));
            require(hits.get(100L).size()==2,"Capped excluded query must produce exactly two distinct hits");
            store.addToDeleteQueue(100,41,82,4);store.processDeleteQueue();
            try(ResultSet r=st.executeQuery("SELECT count(*) FROM panako_fingerprint_search WHERE resource_id=41")) {r.next();require(r.getInt(1)==0,"Individual delete");}
            store.deleteMetadata(42);
            require(store.getMetadata(42)==null,"Resource metadata deleted");
            try(ResultSet r=st.executeQuery("SELECT count(*) FROM panako_fingerprint_by_resource WHERE resource_id=42")) {r.next();require(r.getInt(1)==0,"Reverse resource deleted");}
            store.clear();
            try(ResultSet r=st.executeQuery("SELECT count(*) FROM panako_fingerprint_search")) {r.next();require(r.getInt(1)==0,"Clear packed and buffer");}
            store.close();
        }
        System.out.println("JAVA COMPACT STORAGE: PASS");
    }
}
