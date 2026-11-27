// SPDX-License-Identifier: AGPL-3.0-or-later
package be.panako.strategy.panako.storage;

import be.panako.util.Config;
import be.panako.util.Key;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Exact ordered comparisons with the prior compact lookup in an isolated local DB. */
public final class CompactMergeQueryCheck {
    private static void require(boolean ok,String message){if(!ok)throw new AssertionError(message);}
    private static String tuple(PanakoHit h){return h.matchedNearHash+":"+h.resourceID+":"+h.t+":"+h.f;}
    private static void compare(Connection c,PanakoStoragePostgres store,List<Long> hashes,int range,int cap,Set<Integer> avoid)throws SQLException{
        Config.set(Key.PANAKO_PG_MAX_HITS_PER_HASH,Integer.toString(cap));
        Map<Long,List<String>> expected=new HashMap<>();
        try(PreparedStatement st=c.prepareStatement("SELECT hash,resource_id,t,f FROM panako_compact_hits(?,?,?,?)")){
            for(long hash:hashes){
                st.setLong(1,hash-range);st.setLong(2,hash+range);
                st.setArray(3,c.createArrayOf("integer",avoid.toArray(new Integer[0])));st.setInt(4,cap);
                try(ResultSet r=st.executeQuery()){while(r.next())expected.computeIfAbsent(hash,k->new ArrayList<>()).add(r.getLong(1)+":"+r.getInt(2)+":"+r.getInt(3)+":"+r.getInt(4));}
            }
        }
        Map<Long,List<PanakoHit>> actual=new HashMap<>();
        for(long hash:hashes)store.addToQueryQueue(hash);
        store.processQueryQueue(actual,range,avoid);
        Map<Long,List<String>> normalized=new HashMap<>();
        actual.forEach((hash,hits)->{List<String> rows=new ArrayList<>();for(PanakoHit h:hits){require(h.originalHash==hash,"Query hash provenance");rows.add(tuple(h));}normalized.put(hash,rows);});
        require(expected.equals(normalized),"Ordered lookup differs: range="+range+", cap="+cap+", avoid="+avoid);
    }
    public static void main(String[] args)throws Exception{
        String url=System.getenv("PANAKO_PG_URL");
        require(url!=null&&url.matches("jdbc:postgresql://127\\.0\\.0\\.1:[0-9]+/ear_storage_benchmark_engine_merge_[a-z0-9_]+"),"Use an isolated local merge-query test DB");
        require("true".equalsIgnoreCase(System.getenv("PANAKO_PG_COMPACT")),"Enable compact mode");
        try{new PanakoStoragePostgres();}catch(IllegalStateException expected){}
        try(Connection c=DriverManager.getConnection(url,System.getenv("PANAKO_PG_USER"),"");Statement st=c.createStatement()){
            st.execute(Files.readString(Path.of(args[0])));
            PanakoStoragePostgres store=new PanakoStoragePostgres();store.clear();
            // Dense neighbors, signed IDs, equal IDs with distinct times/frequencies,
            // and exact duplicates between the packed data and the recent buffer.
            for(long h=98;h<=103;h++)for(int i=-20;i<2020;i++)store.addToStoreQueue(h,i,i+30,100+(i&3));
            store.addToStoreQueue(104,Integer.MIN_VALUE,10,100);
            store.addToStoreQueue(104,Integer.MAX_VALUE,20,100);
            store.processStoreQueue();st.execute("SELECT panako_compact_partition('panako_fingerprint_p000')");
            for(long h=98;h<=104;h++)for(int i=-30;i<30;i++)store.addToStoreQueue(h,i,i+30,100+(i&3));
            store.addToStoreQueue(100,-20,11,99);store.addToStoreQueue(100,-20,11,100);store.processStoreQueue();
            List<Long> hashes=Arrays.asList(-10L,97L,98L,99L,100L,101L,102L,103L,104L,105L,109L,100L,104L);
            int cases=0;
            for(int range:new int[]{-1,0,1,2,4})for(int cap:new int[]{-1,0,1,2,7,2000})for(Set<Integer> avoid:List.of(Set.<Integer>of(),Set.of(-30,-20,0,1,Integer.MIN_VALUE,Integer.MAX_VALUE))){compare(c,store,hashes,range,cap,avoid);cases++;}
            // A packed duplicate is treated exactly like the old per-branch cap:
            // deduplication happens after each branch contributes its capped rows.
            st.execute("INSERT INTO panako_fingerprint_hash_packed VALUES (200,ARRAY[1,1,2,3],ARRAY[10,10,20,30],ARRAY[100,100,100,100])");
            store.addToStoreQueue(200,1,10,100);store.addToStoreQueue(200,4,40,100);store.processStoreQueue();
            for(int cap:new int[]{1,2,3,4,0}){compare(c,store,List.of(200L,200L,199L),1,cap,Set.of());cases++;}
            List<Long> boundary=new ArrayList<>();for(int i=0;i<2017;i++)boundary.add(i%3==0?104L:100L);
            compare(c,store,boundary,2,2,Set.of(-30));cases++;
            compare(c,store,List.of(),2,2000,Set.of());cases++;
            // The direct helper dependency must fail closed during startup,
            // even if the older public lookup function is still registered.
            st.execute("ALTER FUNCTION panako_compact_packed_hits(bigint,bigint,integer[],integer) RENAME TO hidden_compact_packed_hits");
            try {
                boolean refused=false;
                try { PanakoStoragePostgres unexpected=new PanakoStoragePostgres();unexpected.close(); }
                catch(IllegalStateException expected){refused=true;}
                require(refused,"Missing packed helper must fail startup");
            } finally {
                st.execute("ALTER FUNCTION hidden_compact_packed_hits(bigint,bigint,integer[],integer) RENAME TO panako_compact_packed_hits");
            }
            store.close();System.out.println("COMPACT MERGE QUERY: PASS ("+cases+" exact ordered comparisons; dense neighbors, caps, exclusions, duplicates, signed IDs, empty lookups, chunk-boundary multiplicity and missing-helper refusal)");
        }
    }
}
