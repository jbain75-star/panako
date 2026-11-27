// SPDX-License-Identifier: AGPL-3.0-or-later
package be.panako.strategy.panako.storage;
import java.sql.*;import java.nio.*;import java.nio.file.*;import java.security.*;import java.util.*;
/** Explicit read-only benchmark of retained query hashes; never stores or decodes audio. */
public class CompactLookupBenchmark {
 public static void main(String[] args)throws Exception {
  if(args.length!=1)throw new IllegalArgumentException("Pass one retained query fingerprint file");
  List<Long> hashes=new ArrayList<>();for(String line:Files.readAllLines(Path.of(args[0])))hashes.add(Long.parseLong(line.trim().split("\\s+")[0]));
  Properties properties=new Properties();String user=System.getenv("PANAKO_PG_USER"),password=System.getenv("PANAKO_PG_PASSWORD");if(user!=null&&!user.isEmpty())properties.setProperty("user",user);if(password!=null&&!password.isEmpty())properties.setProperty("password",password);properties.setProperty("prepareThreshold","3");
  try(Connection c=DriverManager.getConnection(System.getenv("PANAKO_PG_URL"),properties)){
   c.setReadOnly(true);c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false);
   try(Statement s=c.createStatement()){s.execute("SET LOCAL statement_timeout='90s'");}
   String old="SELECT q.query_hash,f.hash,f.resource_id,f.t,f.f FROM unnest(?) q(query_hash) JOIN LATERAL panako_compact_hits(q.query_hash - ?,q.query_hash + ?,?,?) f ON true";
   for(String variant:new String[]{"old","merge","merge","old"}) {
    long started=System.nanoTime(),rows=0;MessageDigest digest=MessageDigest.getInstance("SHA-256");ByteBuffer bytes=ByteBuffer.allocate(28);
    for(int start=0;start<hashes.size();start+=2000){List<Long> chunk=hashes.subList(start,Math.min(start+2000,hashes.size()));try(PreparedStatement s=c.prepareStatement(variant.equals("old")?old:PanakoStoragePostgres.COMPACT_QUERY_SQL)){
     s.setArray(1,c.createArrayOf("bigint",chunk.toArray(new Long[0])));s.setLong(2,2);s.setLong(3,2);s.setArray(4,c.createArrayOf("integer",new Integer[0]));s.setInt(5,2000);s.setFetchSize(10000);
     try(ResultSet r=s.executeQuery()){while(r.next()){bytes.clear();bytes.putLong(r.getLong(1)).putLong(r.getLong(2)).putInt(r.getInt(3)).putInt(r.getInt(4)).putInt(r.getInt(5));digest.update(bytes.array());rows++;}}
    }}
    System.out.printf(Locale.ROOT,"{\"variant\":\"%s\",\"queryHashes\":%d,\"rows\":%d,\"elapsedMs\":%.3f,\"digest\":\"%s\"}%n",variant,hashes.size(),rows,(System.nanoTime()-started)/1e6,Base64.getEncoder().encodeToString(digest.digest()));System.out.flush();
   }
   c.rollback();
  }
 }
}
