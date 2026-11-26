// SPDX-License-Identifier: AGPL-3.0-or-later
package be.panako.strategy.panako.storage;

import be.panako.util.Config;
import be.panako.util.Key;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/** Real compact-index regression in a dedicated local database; no audio required. */
public final class CandidateRefinementCheck {
    static void require(boolean ok,String message){if(!ok)throw new AssertionError(message);}
    static Map<Long,List<PanakoHit>> broad(PanakoStoragePostgres store,List<Long> hashes,Set<Integer> avoid){
        Map<Long,List<PanakoHit>> result=new HashMap<>();
        for(long hash:hashes)store.addToQueryQueue(hash);
        store.processQueryQueue(result,0,avoid);return result;
    }
    static long count(Map<Long,List<PanakoHit>> hits,int id){return hits.values().stream().flatMap(List::stream).filter(h->h.resourceID==id).count();}
    public static void main(String[] args)throws Exception{
        String url=System.getenv("PANAKO_PG_URL");
        require(url!=null&&url.matches("jdbc:postgresql://127\\.0\\.0\\.1:[0-9]+/ear_storage_benchmark_engine_refine_[a-z0-9_]+"),"Use an isolated local refinement test DB");
        require("true".equalsIgnoreCase(System.getenv("PANAKO_PG_COMPACT")),"Enable compact mode");
        Config.set(Key.PANAKO_PG_MAX_HITS_PER_HASH,"2");
        try{new PanakoStoragePostgres();}catch(IllegalStateException expected){}
        try(Connection c=DriverManager.getConnection(url,System.getenv("PANAKO_PG_USER"),"");Statement st=c.createStatement()){
            st.execute(Files.readString(Path.of(args[0])));
            PanakoStoragePostgres store=new PanakoStoragePostgres();
            store.clear();
            List<Long> hashes=new ArrayList<>();
            for(long hash=100;hash<120;hash++){
                hashes.add(hash);
                if(hash<110){store.addToStoreQueue(hash,1,(int)hash,100);store.addToStoreQueue(hash,2,(int)hash,100);}
                store.addToStoreQueue(hash,42,(int)hash+400,100);
            }
            store.processStoreQueue();st.execute("SELECT panako_compact_partition('panako_fingerprint_p000')");
            // A duplicate packed/buffer tuple and one genuinely new buffer hit.
            store.addToStoreQueue(119,42,519,100);store.addToStoreQueue(119,42,700,100);store.processStoreQueue();
            Map<Long,List<PanakoHit>> initial=broad(store,hashes,Set.of());
            require(count(initial,42)==11,"Broad cap must discard target evidence");
            Config.set(Key.PANAKO_PG_REFINE_CANDIDATES,"0");
            require(store.refineQuery(hashes,initial,0,Set.of()).isEmpty(),"Disabled mode must stay unchanged");
            Config.set(Key.PANAKO_PG_REFINE_CANDIDATES,"8");
            Map<Long,List<PanakoHit>> complete=store.refineQuery(hashes,initial,0,Set.of());
            require(count(complete,42)==21,"Recover packed and buffer evidence without double-counting");
            require(count(initial,42)==11,"Do not mutate broad evidence");
            require(count(store.refineQuery(hashes,initial,0,Set.of(42)),42)==0,"Respect exclusions independently");
            List<Long> repeated=new ArrayList<>(hashes);repeated.add(119L);
            require(count(store.refineQuery(repeated,initial,0,Set.of()),42)==23,"Preserve duplicate-query multiplicity");
            require(store.refineQuery(Collections.nCopies(4001,100L),initial,0,Set.of()).isEmpty(),"Bound long queries");
            require(store.refineQuery(hashes,initial,5,Set.of()).isEmpty(),"Bound wide searches");
            Map<Long,List<PanakoHit>> noisy=new HashMap<>();
            for(long h=0;h<12;h++){
                List<PanakoHit> rows=new ArrayList<>();
                for(int id=10;id<21;id++)rows.add(new PanakoHit(h,h,h,id,100));
                if(h==0)for(int i=0;i<1000;i++)rows.add(new PanakoHit(h,h,i,99,100));
                noisy.put(h,rows);
            }
            require(PanakoStoragePostgres.refinementCandidates(noisy,100,Set.of(10)).equals(Arrays.asList(11,12,13,14,15,16,17,18)),"Independent hashes, stable ties, exclusions and eight-candidate ceiling");
            // A secondary query timeout must preserve the original answer and
            // leave the reused database connection healthy for the next query.
            c.setAutoCommit(false);st.execute("LOCK TABLE panako_fingerprint_resource_packed IN ACCESS EXCLUSIVE MODE");
            long started=System.nanoTime();
            require(store.refineQuery(hashes,initial,0,Set.of()).isEmpty(),"Timed-out verification falls back");
            require((System.nanoTime()-started)/1e9<5,"Verification has a small deadline");
            c.rollback();c.setAutoCommit(true);
            require(count(store.refineQuery(hashes,initial,0,Set.of()),42)==21,"Connection recovers after timeout");
            store.close();
        }
        System.out.println("CANDIDATE REFINEMENT: PASS (cap recovery, packed/buffer deduplication, exclusions, multiplicity, limits, timeout recovery)");
    }
}
