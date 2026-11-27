/***************************************************************************
*                                                                          *
* Panako - acoustic fingerprinting                                         *
* Copyright (C) 2014 - 2022 - Joren Six / IPEM                             *
*                                                                          *
* This program is free software: you can redistribute it and/or modify     *
* it under the terms of the GNU Affero General Public License as           *
* published by the Free Software Foundation, either version 3 of the       *
* License, or (at your option) any later version.                          *
*                                                                          *
* This program is distributed in the hope that it will be useful,          *
* but WITHOUT ANY WARRANTY; without even the implied warranty of           *
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the            *
* GNU Affero General Public License for more details.                      *
*                                                                          *
* You should have received a copy of the GNU Affero General Public License *
* along with this program.  If not, see <http://www.gnu.org/licenses/>     *
*                                                                          *
****************************************************************************/

package be.panako.strategy.panako.storage;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import be.panako.util.Config;
import be.panako.util.Key;

/**
 * A storage for the Panako algorithm backed by a PostgreSQL database.
 *
 * <p>
 * It exists because the LMDB storage serialises every writer: LMDB allows a
 * single write transaction at a time, process wide, so concurrent
 * {@code panako store} runs queue behind one another no matter how many cores
 * are available. PostgreSQL takes concurrent writers, does not need the whole
 * index in page cache to stay fast, and can actually delete fingerprints.
 * </p>
 *
 * <p>
 * The fingerprint table is range partitioned on the hash. A Panako hash is a
 * packed 34 bit value, so the key space is split in
 * {@link Key#PANAKO_PG_PARTITIONS} equal ranges. Each partition keeps its own
 * b-tree, which is what keeps random index writes fast once the whole index no
 * longer fits in memory: only the partitions being written to need their upper
 * b-tree levels cached.
 * </p>
 *
 * <p>
 * Queries go out one batch at a time: the whole query queue is sent as a single
 * array and joined against the fingerprint table, so a scan costs one round
 * trip rather than one per fingerprint.
 * </p>
 */
public class PanakoStoragePostgres implements PanakoStorage {

	private final static Logger LOG = Logger.getLogger(PanakoStoragePostgres.class.getName());

	/**
	 * The number of ranges the 34 bit hash space is divided in.
	 */
	private static final long HASH_SPACE = 1L << 34;

	private static PanakoStoragePostgres instance;

	private static final Object mutex = new Object();

	/**
	 * Uses a singleton pattern.
	 * @return Returns or creates a storage instance.
	 */
	public synchronized static PanakoStoragePostgres getInstance() {
		if (instance == null) {
			synchronized (mutex) {
				if (instance == null) {
					instance = new PanakoStoragePostgres();
				}
			}
		}
		return instance;
	}

	/**
	 * One connection per thread: the store, delete and query queues are already
	 * kept per thread and a JDBC connection is not thread safe.
	 */
	private final ThreadLocal<Connection> connections = new ThreadLocal<Connection>() {
		@Override
		protected Connection initialValue() {
			return connect();
		}
	};

	private final boolean compact;
	private final Map<Long, List<long[]>> storeQueue;
	private final Map<Long, List<long[]>> deleteQueue;
	private final Map<Long, List<Long>> queryQueue;

	/** How many queried hashes are asked for in one round trip. */
	private static final int QUERY_CHUNK_SIZE = 2_000;

	/** How many rows the driver holds at a time while reading an answer. */
	private static final int QUERY_FETCH_SIZE = 10_000;

	/**
	 * Create a new storage instance and make sure the schema exists.
	 */
	public PanakoStoragePostgres() {
		String compactSetting = configOrEnvironment(Key.PANAKO_PG_COMPACT);
		if (!compactSetting.equalsIgnoreCase("true") && !compactSetting.equalsIgnoreCase("false"))
			throw new IllegalArgumentException("PANAKO_PG_COMPACT must be TRUE or FALSE");
		compact = Boolean.parseBoolean(compactSetting);
		storeQueue = new HashMap<Long, List<long[]>>();
		deleteQueue = new HashMap<Long, List<long[]>>();
		queryQueue = new HashMap<Long, List<Long>>();

		createSchemaIfNeeded();
		checkCompactSchema();
	}

	/**
	 * Configuration that may hold a secret is also accepted from the environment,
	 * so a caller does not have to put a database password on a command line
	 * where every other process on the machine can read it.
	 *
	 * The environment wins over the configuration: several of these keys have a
	 * built-in default — the url defaults to a database on localhost — and a
	 * default is not a decision anyone made. A caller that sets the variable
	 * means it, and would otherwise be connected to a database it never asked
	 * for. An explicit argument still wins, as it is not passed through here.
	 *
	 * @param key the configuration key
	 * @return the environment variable of the same name when it is set and not
	 *         empty, otherwise the configured value.
	 */
	private String configOrEnvironment(Key key) {
		String fromEnvironment = System.getenv(key.name());
		if (fromEnvironment != null && !fromEnvironment.isEmpty())
			return fromEnvironment;
		String value = Config.get(key);
		return value == null ? "" : value;
	}

	private Connection connect() {
		String url = configOrEnvironment(Key.PANAKO_PG_URL);
		Properties properties = new Properties();
		String user = configOrEnvironment(Key.PANAKO_PG_USER);
		String password = configOrEnvironment(Key.PANAKO_PG_PASSWORD);
		if (!user.isEmpty()) {
			properties.setProperty("user", user);
		}
		if (!password.isEmpty()) {
			properties.setProperty("password", password);
		}
		// Server side prepared statements after a handful of executions: the same
		// query shapes are used over and over.
		properties.setProperty("prepareThreshold", "3");
		try {
			Connection connection = DriverManager.getConnection(url, properties);
			connection.setAutoCommit(true);
			return connection;
		} catch (SQLException e) {
			throw new RuntimeException("Could not connect to the PostgreSQL fingerprint store at " + url, e);
		}
	}

	/**
	 * A connection that outlives a single command does not always survive it. A
	 * socket error, or a thread interrupted while it waits on an answer, leaves
	 * the driver holding a connection it has already closed, and every later
	 * query on that thread fails for as long as the process lives. Handing back
	 * a fresh one costs a connect on the rare occasion it happens.
	 */
	private Connection connection() {
		Connection connection = connections.get();
		try {
			if (!connection.isClosed())
				return connection;
		} catch (SQLException e) {
			// A connection that cannot say whether it is closed is not usable.
		}
		connections.remove();
		return connections.get();
	}

	/**
	 * The connection of the calling thread, for the migration in this package. It
	 * needs the raw connection to copy rows in bulk and to keep a batch and its
	 * resume point in one transaction, neither of which the storage interface
	 * expresses.
	 *
	 * @return the connection of the current thread.
	 */
	Connection migrationConnection() {
		if (compact) throw new IllegalStateException("Legacy LMDB migration requires an ordinary staging database before compaction");
		return connection();
	}

	private void createSchemaIfNeeded() {
		int partitions = Config.getInt(Key.PANAKO_PG_PARTITIONS);
		if (partitions < 1) {
			throw new RuntimeException("PANAKO_PG_PARTITIONS needs to be at least one, is " + partitions);
		}
		// Every process opening the store passes through here, and CREATE INDEX
		// takes a share lock on its table before it finds out the index already
		// exists. Under a steady stream of inserts that lock waits for every
		// batch in flight, and every batch behind it waits for the lock, so the
		// mere check stalled the whole partition for seconds each time a JVM
		// started. Ask the catalog instead: reading it locks nothing, and only
		// what is truly missing is created.
		Set<String> present = existingRelations();
		try (Statement statement = connection().createStatement()) {
			if (present.contains("panako_resource") && present.contains("panako_fingerprint")
					&& hasAllPartitions(present, partitions)) {
				LOG.fine("PostgreSQL fingerprint schema already present");
				return;
			}
			statement.execute("CREATE TABLE IF NOT EXISTS panako_resource ("
					+ "resource_id bigint PRIMARY KEY,"
					+ "path text NOT NULL,"
					+ "duration real NOT NULL,"
					+ "fingerprints integer NOT NULL)");

			statement.execute("CREATE TABLE IF NOT EXISTS panako_fingerprint ("
					+ "hash bigint NOT NULL,"
					+ "resource_id integer NOT NULL,"
					+ "t integer NOT NULL,"
					+ "f integer NOT NULL) PARTITION BY RANGE (hash)");

			long step = HASH_SPACE / partitions;
			for (int i = 0; i < partitions; i++) {
				long from = i * step;
				// The last partition takes whatever is left over, and anything above the
				// 34 bit space, so no hash can ever fail to find a partition.
				String to = (i == partitions - 1) ? "MAXVALUE" : Long.toString(from + step);
				String name = partitionName(i);
				if (!present.contains(name)) {
					statement.execute(String.format(
							"CREATE TABLE IF NOT EXISTS %s PARTITION OF panako_fingerprint "
							+ "FOR VALUES FROM (%s) TO (%s)",
							name, i == 0 ? "MINVALUE" : Long.toString(from), to));
				}
				// Unique, so that storing the same audio twice cannot double its
				// fingerprints: an identical print for the same resource conflicts and is
				// dropped (see processStoreQueue). Leading on hash, and covering every
				// column, it also answers a query on its own — the same shape the plain
				// covering index had.
				if (!present.contains(indexName(i))) {
					statement.execute(String.format(
							"CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (hash,resource_id,t,f)",
							indexName(i), name));
				}
			}
			LOG.info(String.format("PostgreSQL fingerprint store ready with %d hash partitions", partitions));
		} catch (SQLException e) {
			throw new RuntimeException("Could not create the PostgreSQL fingerprint schema", e);
		}
	}

	private void checkCompactSchema() {
		try (Statement statement = connection().createStatement()) {
			boolean installed;
			try (ResultSet r = statement.executeQuery("SELECT to_regclass('panako_fingerprint_compact_meta') IS NOT NULL")) {
				r.next(); installed = r.getBoolean(1);
			}
			if (!installed) {
				if (compact) throw new IllegalStateException("Install compact-storage.sql before enabling PANAKO_PG_COMPACT");
				return;
			}
			try (ResultSet r = statement.executeQuery("SELECT version FROM panako_fingerprint_compact_meta WHERE singleton")) {
				if (!r.next() || r.getInt(1) != 1) throw new IllegalStateException("Unsupported compact storage schema");
			}
			if (!compact) {
				try (ResultSet r = statement.executeQuery("SELECT EXISTS(SELECT 1 FROM panako_fingerprint_hash_packed)")) {
					r.next();
					if (r.getBoolean(1)) throw new IllegalStateException("Packed fingerprints require PANAKO_PG_COMPACT=TRUE");
				}
			} else {
				// Resolve both views now; never turn an unavailable store into an empty answer.
				statement.executeQuery("SELECT * FROM panako_fingerprint_search LIMIT 0").close();
				statement.executeQuery("SELECT * FROM panako_fingerprint_by_resource LIMIT 0").close();
				try (ResultSet r = statement.executeQuery("SELECT to_regprocedure('panako_compact_hits(bigint,bigint,integer[],integer)') IS NOT NULL "
						+ "AND to_regprocedure('panako_compact_packed_hits(bigint,bigint,integer[],integer)') IS NOT NULL "
						+ "AND to_regprocedure('panako_compact_delete_resource(integer)') IS NOT NULL "
						+ "AND to_regprocedure('panako_compact_delete_fingerprints(bigint[],integer[],integer[],integer[])') IS NOT NULL")) {
					r.next(); if (!r.getBoolean(1)) throw new IllegalStateException("Incomplete compact storage schema");
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Could not validate compact fingerprint storage", e);
		}
	}

	private void lockCompactWriter(Connection connection) throws SQLException {
		if (compact) try (Statement statement = connection.createStatement()) {
			statement.execute("SELECT pg_advisory_xact_lock_shared(179108082)");
		}
	}

	private static String partitionName(int i) {
		return String.format("panako_fingerprint_p%03d", i);
	}

	private static String indexName(int i) {
		return partitionName(i) + "_hash_idx";
	}

	private static boolean hasAllPartitions(Set<String> present, int partitions) {
		for (int i = 0; i < partitions; i++) {
			if (!present.contains(partitionName(i)) || !present.contains(indexName(i))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * The names of the fingerprint schema's tables and indexes that exist, read
	 * from the catalog without touching the tables themselves.
	 */
	private Set<String> existingRelations() {
		Set<String> names = new HashSet<String>();
		String sql = "SELECT c.relname FROM pg_class c "
				+ "JOIN pg_namespace n ON n.oid = c.relnamespace "
				+ "WHERE n.nspname = current_schema() "
				+ "AND c.relkind IN ('r','p','i') "
				+ "AND (c.relname = 'panako_resource' OR c.relname LIKE 'panako\\_fingerprint%')";
		try (Statement statement = connection().createStatement();
				ResultSet result = statement.executeQuery(sql)) {
			while (result.next()) {
				names.add(result.getString(1));
			}
		} catch (SQLException e) {
			throw new RuntimeException("Could not read the PostgreSQL catalog", e);
		}
		return names;
	}

	@Override
	public void storeMetadata(long resourceID, String resourcePath, float duration, int fingerprints) {
		String sql = "INSERT INTO panako_resource (resource_id,path,duration,fingerprints) VALUES (?,?,?,?) "
				+ "ON CONFLICT (resource_id) DO UPDATE SET path = EXCLUDED.path, "
				+ "duration = EXCLUDED.duration, fingerprints = EXCLUDED.fingerprints";
		Connection c = connection();
		try {
			c.setAutoCommit(false);
			lockCompactWriter(c);
			try (PreparedStatement statement = c.prepareStatement(sql)) {
				statement.setLong(1, resourceID);
				statement.setString(2, resourcePath);
				statement.setFloat(3, duration);
				statement.setInt(4, fingerprints);
				statement.executeUpdate();
			}
			c.commit();
		} catch (SQLException e) {
			rollback(c);
			throw new RuntimeException("Could not store meta-data for resource " + resourceID, e);
		} finally {
			autoCommit(c);
		}
	}

	@Override
	public PanakoResourceMetadata getMetadata(long identifier) {
		String sql = "SELECT path,duration,fingerprints FROM panako_resource WHERE resource_id = ?";
		try (PreparedStatement statement = connection().prepareStatement(sql)) {
			statement.setLong(1, identifier);
			try (ResultSet result = statement.executeQuery()) {
				if (!result.next()) {
					return null;
				}
				PanakoResourceMetadata metadata = new PanakoResourceMetadata();
				metadata.path = result.getString(1);
				metadata.duration = result.getFloat(2);
				metadata.numFingerprints = result.getInt(3);
				metadata.identifier = (int) identifier;
				return metadata;
			}
		} catch (SQLException e) {
			throw new RuntimeException("Could not read meta-data for resource " + identifier, e);
		}
	}

	@Override
	public void deleteMetadata(long resourceID) {
		if (compact) {
			try (PreparedStatement statement = connection().prepareStatement("SELECT panako_compact_delete_resource(?)")) {
				statement.setInt(1, Math.toIntExact(resourceID));
				statement.execute();
				return;
			} catch (SQLException e) {
				throw new RuntimeException("Could not delete compact resource " + resourceID, e);
			}
		}
		try (PreparedStatement statement = connection()
				.prepareStatement("DELETE FROM panako_resource WHERE resource_id = ?")) {
			statement.setLong(1, resourceID);
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Could not delete meta-data for resource " + resourceID, e);
		}

		// Deleting a resource by re-extracting its fingerprints only removes the
		// prints the current audio yields. Anything stored for this resource by an
		// earlier, different version of the same audio - a preview replaced by the
		// full recording, say - would survive as orphans. Remove those here: no
		// fingerprint of a resource without meta-data can ever be resolved.
		try (PreparedStatement statement = connection()
				.prepareStatement("DELETE FROM panako_fingerprint WHERE resource_id = ?")) {
			statement.setInt(1, (int) resourceID);
			int removed = statement.executeUpdate();
			if (removed > 0) {
				LOG.info(String.format("Removed %d remaining fingerprints of resource %d", removed, resourceID));
			}
		} catch (SQLException e) {
			throw new RuntimeException("Could not delete fingerprints for resource " + resourceID, e);
		}
	}

	@Override
	public void addToStoreQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
		long[] data = { fingerprintHash, resourceIdentifier, t1, f1 };
		long threadID = Thread.currentThread().getId();
		if (!storeQueue.containsKey(threadID))
			storeQueue.put(threadID, new ArrayList<long[]>());
		storeQueue.get(threadID).add(data);
	}

	@Override
	public void processStoreQueue() {
		List<long[]> queue = queueForCurrentThread(storeQueue);
		if (queue == null)
			return;

		// DO NOTHING rather than plain INSERT: storing audio that is already in the
		// store is a normal thing to ask for — a re-print of the same file, a repair
		// pass — and it must leave the store the size it was, not twice the size with
		// every hit counted twice.
		String sql = "INSERT INTO panako_fingerprint (hash,resource_id,t,f) VALUES (?,?,?,?) "
				+ "ON CONFLICT DO NOTHING";
		int batchSize = Config.getInt(Key.PANAKO_PG_BATCH_SIZE);
		Connection connection = connection();
		try {
			connection.setAutoCommit(false);
			lockCompactWriter(connection);
			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				int inBatch = 0;
				for (long[] data : queue) {
					statement.setLong(1, data[0]);
					statement.setInt(2, (int) data[1]);
					statement.setInt(3, (int) data[2]);
					statement.setInt(4, (int) data[3]);
					statement.addBatch();
					if (++inBatch % batchSize == 0) {
						statement.executeBatch();
					}
				}
				statement.executeBatch();
			}
			connection.commit();
			queue.clear();
		} catch (SQLException e) {
			rollback(connection);
			throw new RuntimeException("Could not store fingerprints", e);
		} finally {
			autoCommit(connection);
		}
	}

	@Override
	public void addToDeleteQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
		long[] data = { fingerprintHash, resourceIdentifier, t1, f1 };
		long threadID = Thread.currentThread().getId();
		if (!deleteQueue.containsKey(threadID))
			deleteQueue.put(threadID, new ArrayList<long[]>());
		deleteQueue.get(threadID).add(data);
	}

	@Override
	public void processDeleteQueue() {
		List<long[]> queue = queueForCurrentThread(deleteQueue);
		if (queue == null)
			return;

		if (compact) {
			deleteCompactQueue(queue);
			return;
		}
		String sql = "DELETE FROM panako_fingerprint WHERE hash = ? AND resource_id = ? AND t = ? AND f = ?";
		int batchSize = Config.getInt(Key.PANAKO_PG_BATCH_SIZE);
		Connection connection = connection();
		try {
			connection.setAutoCommit(false);
			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				int inBatch = 0;
				for (long[] data : queue) {
					statement.setLong(1, data[0]);
					statement.setInt(2, (int) data[1]);
					statement.setInt(3, (int) data[2]);
					statement.setInt(4, (int) data[3]);
					statement.addBatch();
					if (++inBatch % batchSize == 0) {
						statement.executeBatch();
					}
				}
				statement.executeBatch();
			}
			connection.commit();
			queue.clear();
		} catch (SQLException e) {
			rollback(connection);
			throw new RuntimeException("Could not delete fingerprints", e);
		} finally {
			autoCommit(connection);
		}
	}

	private void deleteCompactQueue(List<long[]> queue) {
		Connection c = connection();
		Array[] arrays = new Array[4];
		try {
			c.setAutoCommit(false);
			Long[] hashes = new Long[queue.size()];
			Integer[][] values = new Integer[3][queue.size()];
			for (int i = 0; i < queue.size(); i++) {
				hashes[i] = queue.get(i)[0];
				for (int j = 0; j < 3; j++) values[j][i] = (int) queue.get(i)[j + 1];
			}
			arrays[0] = c.createArrayOf("bigint", hashes);
			for (int j = 0; j < 3; j++) arrays[j + 1] = c.createArrayOf("integer", values[j]);
			try (PreparedStatement statement = c.prepareStatement("SELECT panako_compact_delete_fingerprints(?,?,?,?)")) {
				for (int j = 0; j < 4; j++) statement.setArray(j + 1, arrays[j]);
				statement.execute();
			}
			c.commit();
			queue.clear();
		} catch (SQLException e) {
			rollback(c);
			throw new RuntimeException("Could not delete compact fingerprints", e);
		} finally {
			for (Array array : arrays) if (array != null) try { array.free(); } catch (SQLException ignored) { }
			autoCommit(c);
		}
	}

	@Override
	public void addToQueryQueue(long queryHash) {
		long threadID = Thread.currentThread().getId();
		if (!queryQueue.containsKey(threadID))
			queryQueue.put(threadID, new ArrayList<Long>());
		queryQueue.get(threadID).add(queryHash);
	}

    /** Ordered merge of the same capped packed and recent tuples; one SQL snapshot. */
    static final String COMPACT_QUERY_SQL = "SELECT q.query_hash,f.hash,f.resource_id,f.t,f.f FROM unnest(?) q(query_hash) "
                    + "CROSS JOIN LATERAL (SELECT q.query_hash - ? AS low_hash, q.query_hash + ? AS high_hash, "
                    + "?::integer[] AS avoid, ?::integer AS max_hits) bounds "
                    + "JOIN LATERAL (SELECT DISTINCT ON (hash,resource_id,t,f) * FROM ("
                    + "(SELECT h.hash,h.resource_id,h.t,h.f FROM panako_fingerprint h "
                    + "WHERE h.hash BETWEEN bounds.low_hash AND bounds.high_hash AND h.resource_id <> ALL(bounds.avoid) "
                    + "ORDER BY h.hash,h.resource_id,h.t,h.f LIMIT nullif(greatest(bounds.max_hits,0),0)) "
                    + "UNION ALL SELECT * FROM panako_compact_packed_hits(bounds.low_hash,bounds.high_hash,bounds.avoid,bounds.max_hits)"
                    + ") hits ORDER BY hash,resource_id,t,f LIMIT nullif(greatest(bounds.max_hits,0),0)) f ON true";

	@Override
	public void processQueryQueue(Map<Long, List<PanakoHit>> matchAccumulator, int range) {
		processQueryQueue(matchAccumulator, range, new HashSet<Integer>());
	}

	@Override
	public void processQueryQueue(Map<Long, List<PanakoHit>> matchAccumulator, int range,
			Set<Integer> resourcesToAvoid) {
		List<Long> queue = queueForCurrentThread(queryQueue);
		if (queue == null)
			return;

		// The whole queue leaves as one array so a query costs a single round trip:
		// each queried hash is joined against the hashes within range of it.
		// The join is lateral so each hash's answer can be cut off: in an index
		// of billions of fingerprints a hash is shared by thousands of recordings,
		// and reading every one of them is what a query spends its time on. A
		// hash heard in that many recordings barely tells them apart, so the
		// rows left behind are mostly noise (see the key for what a match loses).
		int maxHits = Config.getInt(Key.PANAKO_PG_MAX_HITS_PER_HASH);
		String sql;
		if (compact) {
            // Both branches already provide their first K eligible tuples in
            // canonical order. DISTINCT ON lets PostgreSQL merge those streams,
            // deduplicate adjacent equal tuples, and stop at K. UNION otherwise
            // hashes every tuple and sorts the entire union for every hash.
            // Keep this in one statement: a concurrent compaction must not move
            // a tuple between the two reads' snapshots and make it disappear.
            sql = COMPACT_QUERY_SQL;
		} else {
			sql = "SELECT q.query_hash, f.hash, f.resource_id, f.t, f.f "
					+ "FROM unnest(?) AS q(query_hash) "
					+ "JOIN LATERAL (SELECT hash, resource_id, t, f FROM panako_fingerprint "
					+ "WHERE hash BETWEEN q.query_hash - ? AND q.query_hash + ?";
			if (!resourcesToAvoid.isEmpty()) sql += " AND NOT (resource_id = ANY(?))";
			if (maxHits > 0) sql += " LIMIT " + maxHits;
			sql += ") f ON true";
		}

		Connection connection = connection();
		try {
			// A long recording asks tens of thousands of hashes at once, and each
			// one can be answered by many rows. Asked as a single statement in
			// auto-commit, the driver holds every row of the answer in memory
			// before the first is read, which is what exhausts the heap on long
			// records. A cursor needs a transaction, and the queue leaves in
			// chunks so no one answer is unbounded.
			connection.setAutoCommit(false);
			for (int start = 0; start < queue.size(); start += QUERY_CHUNK_SIZE) {
				List<Long> chunk = queue.subList(start,
						Math.min(start + QUERY_CHUNK_SIZE, queue.size()));
				Array hashArray = connection.createArrayOf("bigint", chunk.toArray(new Long[0]));
				try (PreparedStatement statement = connection.prepareStatement(sql)) {
					statement.setArray(1, hashArray);
					statement.setLong(2, range);
					statement.setLong(3, range);
					if (compact || !resourcesToAvoid.isEmpty()) {
						statement.setArray(4, connection.createArrayOf("integer",
								resourcesToAvoid.toArray(new Integer[0])));
					}
					if (compact) statement.setInt(5, maxHits);
					statement.setFetchSize(QUERY_FETCH_SIZE);
					try (ResultSet result = statement.executeQuery()) {
						while (result.next()) {
							long originalKey = result.getLong(1);
							long fingerprintHash = result.getLong(2);
							long resourceID = result.getInt(3);
							long t = result.getInt(4);
							long f = result.getInt(5);
							if (!matchAccumulator.containsKey(originalKey))
								matchAccumulator.put(originalKey, new ArrayList<PanakoHit>());
							matchAccumulator.get(originalKey)
									.add(new PanakoHit(originalKey, fingerprintHash, t, resourceID, f));
						}
					}
				}
			}
			connection.commit();
		} catch (SQLException e) {
			rollback(connection);
			throw new RuntimeException("Could not query fingerprints", e);
		} finally {
			// The hashes belong to the query that is over, answered or not. Left
			// behind, they are asked again alongside the next query's own, and the
			// hits they bring back belong to audio the caller is no longer holding.
			queue.clear();
			autoCommit(connection);
		}
	}

    private static final int MAX_REFINEMENT_HASHES = 2_000;
    private static final int MAX_REFINEMENT_ROWS = 100_000;
    private static final int MAX_REFINEMENT_HITS = 500_000;

    /** Rank independent queried hashes, not repeated hits from one beat. */
    static List<Integer> refinementCandidates(Map<Long,List<PanakoHit>> hits,
            int limit, Set<Integer> avoid) {
        Map<Integer,Integer> votes = new HashMap<>();
        Set<Integer> seen = new HashSet<>();
        for (List<PanakoHit> group : hits.values()) {
            seen.clear();
            for (PanakoHit hit : group)
                if (!avoid.contains(hit.resourceID) && seen.add(hit.resourceID))
                    votes.merge(hit.resourceID, 1, Integer::sum);
        }
        List<Integer> ranked = new ArrayList<>();
        for (Map.Entry<Integer,Integer> vote : votes.entrySet())
            if (vote.getValue() >= 6) ranked.add(vote.getKey());
        ranked.sort((a,b) -> {
            int byVotes = Integer.compare(votes.get(b), votes.get(a));
            return byVotes == 0 ? Integer.compare(a,b) : byVotes;
        });
        return new ArrayList<>(ranked.subList(0, Math.min(Math.max(0, Math.min(8,limit)), ranked.size())));
    }

    @Override
    public Map<Long,List<PanakoHit>> refineQuery(List<Long> queryHashes,
            Map<Long,List<PanakoHit>> broadHits, int range, Set<Integer> avoid) {
        int limit = Config.getInt(Key.PANAKO_PG_REFINE_CANDIDATES);
        // Only the compact layout has a resource-keyed fingerprint index.
        // Keep long recordings and unusually wide searches on the existing path.
        if (!compact || limit <= 0 || range < 0 || range > 4 || queryHashes.size() > 4_000)
            return java.util.Collections.emptyMap();
        Set<Long> unique = new HashSet<>(queryHashes);
        if (unique.isEmpty() || unique.size() > MAX_REFINEMENT_HASHES)
            return java.util.Collections.emptyMap();
        List<Integer> candidates = refinementCandidates(broadHits, limit, avoid);
        if (candidates.isEmpty()) return java.util.Collections.emptyMap();
        Set<Long> neighbours = new HashSet<>();
        for (long hash : unique) for (int d=-range; d<=range; d++) neighbours.add(hash+d);
        // Both halves are bounded by an index: packed resources by their primary
        // key, and recent ordinary rows by queried hash. Never scan the ordinary
        // table by resource_id alone. UNION removes rows present in both stores.
        String sql = "SELECT hash,resource_id,t,f FROM ("
                + "SELECT p.hash,r.resource_id,p.t,p.f FROM panako_fingerprint_resource_packed r "
                + "CROSS JOIN LATERAL unnest(r.hashes,r.times,r.freqs) p(hash,t,f) "
                + "WHERE r.resource_id=ANY(?) AND p.hash=ANY(?) UNION "
                + "SELECT hash,resource_id,t,f FROM panako_fingerprint "
                + "WHERE hash=ANY(?) AND resource_id=ANY(?)) selected LIMIT " + (MAX_REFINEMENT_ROWS+1);
        Connection c = connection();
        long started = System.nanoTime();
        try {
            c.setAutoCommit(false);
            Map<Long,List<long[]>> byHash = new HashMap<>();
            Array ids = c.createArrayOf("integer", candidates.toArray(new Integer[0]));
            Array hashes = c.createArrayOf("bigint", neighbours.toArray(new Long[0]));
            try (PreparedStatement st = c.prepareStatement(sql)) {
                st.setArray(1,ids); st.setArray(2,hashes); st.setArray(3,hashes); st.setArray(4,ids);
                st.setQueryTimeout(3); st.setFetchSize(10_000);
                int rows=0;
                try (ResultSet r = st.executeQuery()) {
                    while (r.next()) {
                        if (++rows > MAX_REFINEMENT_ROWS) return java.util.Collections.emptyMap();
                        byHash.computeIfAbsent(r.getLong(1), ignored -> new ArrayList<>())
                                .add(new long[] {r.getLong(1),r.getInt(2),r.getInt(3),r.getInt(4)});
                    }
                }
            } finally { ids.free(); hashes.free(); }
            c.commit();
            Map<Long,List<PanakoHit>> result = new HashMap<>();
            int total=0;
            // Preserve the broad query's multiplicity and last-print semantics.
            for (long hash : queryHashes) for (int d=-range; d<=range; d++) {
                List<long[]> rows = byHash.get(hash+d);
                if (rows == null) continue;
                for (long[] row : rows) {
                    if (++total > MAX_REFINEMENT_HITS) return java.util.Collections.emptyMap();
                    result.computeIfAbsent(hash, ignored -> new ArrayList<>())
                            .add(new PanakoHit(hash,row[0],row[2],row[1],row[3]));
                }
            }
            LOG.info(String.format("Verified %d shortlisted resources with %d hits in %.1f ms",
                    candidates.size(),total,(System.nanoTime()-started)/1_000_000.0));
            return result;
        } catch (SQLException e) {
            LOG.warning("Candidate verification unavailable; retaining broad-search results");
            return java.util.Collections.emptyMap();
        } finally {
            // Also covers an over-budget result and a cancelled statement.
            rollback(c); autoCommit(c);
        }
    }

	@Override
	public void printStatistics(boolean detailedStats) {
		try (Statement statement = connection().createStatement()) {
			long fingerprints = 0;
			long resources = 0;
			double totalDuration = 0;
			try (ResultSet result = statement.executeQuery(
					"SELECT count(*), coalesce(sum(duration),0), coalesce(sum(fingerprints),0) FROM panako_resource")) {
				if (result.next()) {
					resources = result.getLong(1);
					totalDuration = result.getDouble(2);
					fingerprints = result.getLong(3);
				}
			}

			if (detailedStats) {
				long tableBytes = 0;
				// The parent of a partitioned table holds no data itself: size is the
				// sum over its partitions.
				try (ResultSet result = statement.executeQuery(
						"SELECT coalesce(sum(pg_total_relation_size(child.oid)),0) "
						+ "FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid "
						+ "JOIN pg_class child ON pg_inherits.inhrelid = child.oid "
						+ "WHERE parent.relname = 'panako_fingerprint'")) {
					if (result.next())
						tableBytes = result.getLong(1);
				}
				if (compact) try (ResultSet packed = statement.executeQuery(
						"SELECT pg_total_relation_size('panako_fingerprint_hash_packed') + pg_total_relation_size('panako_fingerprint_resource_packed')")) {
					packed.next(); tableBytes += packed.getLong(1);
				}
				System.out.printf("[POSTGRES INDEX statistics]\n");
				System.out.printf("=========================\n");
				System.out.printf("> Hash partitions:              %d\n", Config.getInt(Key.PANAKO_PG_PARTITIONS));
				System.out.printf("> Size of fingerprint table:    %dMB\n", tableBytes / (1024 * 1024));
				System.out.printf("=========================\n\n");
			}

			System.out.printf("[POSTGRES INDEX TOTALS]\n");
			System.out.printf("=========================\n");
			System.out.printf("> %d audio files \n", resources);
			System.out.printf("> %.3f seconds of audio\n", totalDuration);
			System.out.printf("> %d fingerprint hashes \n", fingerprints);
			System.out.printf("=========================\n\n");

			if (totalDuration > 0) {
				System.out.printf("[POSTGRES INDEX INFO]\n");
				System.out.printf("=========================\n");
				System.out.printf("> Avg prints per second: %5.1ffp/s \n", fingerprints / totalDuration);
				System.out.printf("=========================\n\n");
			}
		} catch (SQLException e) {
			throw new RuntimeException("Could not read fingerprint store statistics", e);
		}
	}

	@Override
	public void clear() {
		Connection c = connection();
		try {
			c.setAutoCommit(false);
			try (Statement statement = c.createStatement()) {
				if (compact) {
					statement.execute("SELECT pg_advisory_xact_lock(179108082)");
					statement.execute("TRUNCATE panako_fingerprint_hash_packed,panako_fingerprint_resource_packed");
				}
				statement.execute("TRUNCATE panako_fingerprint,panako_resource");
			}
			c.commit();
			System.out.println("Cleared the PostgreSQL fingerprint store");
		} catch (SQLException e) {
			rollback(c);
			throw new RuntimeException("Could not clear the fingerprint store", e);
		} finally {
			autoCommit(c);
		}
	}

	/**
	 * Close the connection of the current thread.
	 */
	public void close() {
		Connection connection = connections.get();
		try {
			connection.close();
		} catch (SQLException e) {
			LOG.warning("Could not close the PostgreSQL connection: " + e.getMessage());
		}
		connections.remove();
	}

	private <T> List<T> queueForCurrentThread(Map<Long, List<T>> queues) {
		if (queues.isEmpty())
			return null;
		List<T> queue = queues.get(Thread.currentThread().getId());
		if (queue == null || queue.isEmpty())
			return null;
		return queue;
	}

	private void rollback(Connection connection) {
		try {
			connection.rollback();
		} catch (SQLException e) {
			LOG.warning("Could not roll back: " + e.getMessage());
		}
	}

	private void autoCommit(Connection connection) {
		try {
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			LOG.warning("Could not restore auto commit: " + e.getMessage());
		}
	}
}
