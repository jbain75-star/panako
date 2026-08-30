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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.CursorIterable.KeyVal;
import org.lmdbjava.GetOp;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Stat;
import org.lmdbjava.Txn;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import be.panako.util.Config;
import be.panako.util.Key;

/**
 * Copies an existing LMDB fingerprint store into a PostgreSQL one.
 *
 * <p>
 * The fingerprints in the LMDB store are the full, already extracted prints of
 * every stored file: they can be moved as data. Re-extracting them from audio
 * would mean decoding and fingerprinting the whole library again, which is
 * weeks of work for an identical result, and impossible for any file whose
 * audio is no longer at hand.
 * </p>
 *
 * <p>
 * The copy is made to be interruptible, because it runs for hours next to a
 * live store:
 * </p>
 * <ul>
 * <li>Fingerprints are read in hash order, one bounded batch per LMDB read
 * transaction, so no read transaction is ever long lived. A long lived reader
 * would pin LMDB's free list and let the file grow for as long as the copy
 * runs.</li>
 * <li>A batch always ends on a hash boundary and the last copied hash is
 * committed together with the rows themselves. A run that is killed resumes
 * from that hash: the only work repeated is the batch that was in flight.</li>
 * <li>Rows are inserted with {@code ON CONFLICT DO NOTHING}, so replaying a
 * batch - or running the whole migration again - cannot duplicate anything.</li>
 * </ul>
 *
 * <p>
 * Nothing is written to or deleted from LMDB. The old store stays complete and
 * serving until someone decides otherwise.
 * </p>
 *
 * <p>
 * A minimum duration leaves the prints of files shorter than it behind: a print
 * of thirty seconds of a six minute record only matches while those thirty
 * seconds play, so it is close to dead weight in the new store. The files it
 * skips are written to {@code panako_migration_short_resource} instead, as the
 * list of what is worth fetching in full.
 * </p>
 */
public class PanakoStorageMigration {

	private final static Logger LOG = Logger.getLogger(PanakoStorageMigration.class.getName());

	/**
	 * Rows per batch, and so per commit and per LMDB read transaction. Half a
	 * million rows is about fifteen megabytes of copy data: large enough that the
	 * per batch overhead disappears, small enough that a batch is seconds of work
	 * and little is lost when the run is interrupted.
	 */
	public static final long DEFAULT_BATCH_SIZE = 500_000;

	private final PanakoStorageKV source;
	private final PanakoStoragePostgres target;
	private final long batchSize;
	private final float minimumDuration;

	/** Resource identifiers left behind, empty when nothing is left behind. */
	private Set<Integer> skipped = new HashSet<Integer>();

	/**
	 * @param source   the LMDB store to read.
	 * @param target   the PostgreSQL store to fill.
	 * @param batchSize the number of fingerprints per commit.
	 * @param minimumDuration the number of seconds a file has to last to be
	 *                        copied; zero copies everything.
	 */
	public PanakoStorageMigration(PanakoStorageKV source, PanakoStoragePostgres target, long batchSize,
			float minimumDuration) {
		this.source = source;
		this.target = target;
		this.batchSize = batchSize < 1 ? DEFAULT_BATCH_SIZE : batchSize;
		this.minimumDuration = minimumDuration;
	}

	/**
	 * Copy meta-data and fingerprints, resuming where an earlier run stopped.
	 */
	public void migrate() {
		Connection connection = target.migrationConnection();
		createProgressTable(connection);
		// Read before anything is written: a resume point that belongs to another
		// store or another threshold has to stop the run while PostgreSQL still
		// holds what the earlier run left there.
		Long resumeAt = readProgress(connection);
		removeLeftBehind(connection);
		recordShortResources(connection);
		long resources = copyMetadata(connection);
		System.out.printf("> Copied meta-data of %d audio files\n", resources);
		copyFingerprints(connection, resumeAt);
	}

	/**
	 * Start over: forget the progress of earlier runs. The fingerprints already in
	 * PostgreSQL are left alone, they conflict and are skipped when met again.
	 */
	public void restart() {
		Connection connection = target.migrationConnection();
		createProgressTable(connection);
		try (Statement statement = connection.createStatement()) {
			statement.execute("DELETE FROM panako_migration_progress");
		} catch (SQLException e) {
			throw new RuntimeException("Could not clear the migration progress", e);
		}
		System.out.println("> Migration progress cleared, the next run starts at the first hash");
	}

	/**
	 * Compare both stores: the number of audio files, the number of fingerprints,
	 * and a sample of individual fingerprints spread over the whole hash space.
	 *
	 * @param sampleSize the number of individual fingerprints to look up.
	 * @return true when both stores agree.
	 */
	public boolean verify(int sampleSize) {
		Connection connection = target.migrationConnection();
		findShortResources();

		long lmdbFingerprints;
		long lmdbResources;
		try (Txn<ByteBuffer> txn = source.env.txnRead()) {
			Stat fingerprintStat = source.fingerprints.stat(txn);
			Stat resourceStat = source.resourceMap.stat(txn);
			lmdbFingerprints = fingerprintStat.entries;
			lmdbResources = resourceStat.entries;
		}

		long pgFingerprints = count(connection, "SELECT count(*) FROM panako_fingerprint");
		long pgResources = count(connection, "SELECT count(*) FROM panako_resource");

		// What is deliberately left behind is not a difference between the stores, so
		// it comes off the numbers the two are held to.
		long expectedResources = lmdbResources - skipped.size();
		long expectedFingerprints = lmdbFingerprints - countSkippedEntries();

		System.out.printf("> audio files:   lmdb %d, postgres %d\n", expectedResources, pgResources);
		System.out.printf("> fingerprints:  lmdb %d, postgres %d\n", expectedFingerprints, pgFingerprints);
		if (!skipped.isEmpty()) {
			System.out.printf("> left behind:   %d audio files shorter than %.0f seconds\n",
					skipped.size(), minimumDuration);
		}

		// Meta-data is what turns a match into a file name, so every row of it is
		// compared rather than sampled: there are as many as there are audio files.
		int metadataDifferences = compareMetadata(connection);
		System.out.printf("> meta-data:     %d of %d audio files differ\n", metadataDifferences, expectedResources);

		int checked = 0;
		int missing = 0;
		if (sampleSize > 0 && expectedFingerprints > 0) {
			// Every nth fingerprint rather than the first n: the copy walks the hash
			// space in order, so a sample taken from the start would pass while a
			// half finished copy was missing everything above some hash. The stride
			// counts only what should have been copied, so leaving files behind
			// thins the sample rather than the coverage.
			long step = Math.max(1, expectedFingerprints / sampleSize);
			long seen = 0;
			try (Txn<ByteBuffer> txn = source.env.txnRead();
					CursorIterable<ByteBuffer> iterable = source.fingerprints.iterate(txn);
					PreparedStatement statement = connection.prepareStatement(
							"SELECT 1 FROM panako_fingerprint WHERE hash = ? AND resource_id = ? AND t = ? AND f = ?")) {
				for (KeyVal<ByteBuffer> keyVal : iterable) {
					long hash = keyVal.key().order(ByteOrder.LITTLE_ENDIAN).getLong();
					ByteBuffer value = keyVal.val();
					int resourceID = value.getInt();
					int t = value.getInt();
					int f = value.getInt();
					if (skipped.contains(resourceID))
						continue;
					if (seen++ % step != 0)
						continue;
					statement.setLong(1, hash);
					statement.setInt(2, resourceID);
					statement.setInt(3, t);
					statement.setInt(4, f);
					try (ResultSet result = statement.executeQuery()) {
						checked++;
						if (!result.next()) {
							missing++;
							if (missing <= 5) {
								System.out.printf("> missing in postgres: hash %d, resource %d, t %d, f %d\n",
										hash, resourceID, t, f);
							}
						}
					}
					if (checked >= sampleSize)
						break;
				}
			} catch (SQLException e) {
				throw new RuntimeException("Could not compare sampled fingerprints", e);
			}
			System.out.printf("> sampled:       %d fingerprints, %d missing\n", checked, missing);
		}

		// Equal rather than "at least": LMDB keeps one entry per distinct
		// fingerprint and PostgreSQL one row per distinct fingerprint, so a surplus
		// in PostgreSQL is a print of something the source no longer holds - a
		// stale answer waiting to be given.
		boolean equal = expectedResources == pgResources && expectedFingerprints == pgFingerprints && missing == 0
				&& metadataDifferences == 0;
		System.out.println(equal ? "> the stores agree" : "> the stores DO NOT agree");
		return equal;
	}

	/**
	 * The number of fingerprints in LMDB that belong to a file left behind, counted
	 * rather than taken from the meta-data: the meta-data records how many prints
	 * were extracted, which is not the same as how many entries they occupy once
	 * identical ones have collapsed.
	 */
	private long countSkippedEntries() {
		if (skipped.isEmpty())
			return 0;
		long entries = 0;
		try (Txn<ByteBuffer> txn = source.env.txnRead();
				CursorIterable<ByteBuffer> iterable = source.fingerprints.iterate(txn)) {
			for (KeyVal<ByteBuffer> keyVal : iterable) {
				if (skipped.contains(keyVal.val().getInt())) {
					entries++;
				}
			}
		}
		return entries;
	}

	/**
	 * Fill {@link #skipped} with the files that last less than the minimum
	 * duration. A duration of zero or less is unknown rather than short, so such a
	 * file is copied: leaving it behind on a number that was never measured would
	 * throw away a full print.
	 */
	private void findShortResources() {
		skipped = new HashSet<Integer>();
		if (minimumDuration <= 0)
			return;
		for (Object[] row : readMetadata()) {
			float duration = ((Float) row[2]).floatValue();
			if (duration > 0 && duration < minimumDuration) {
				skipped.add(Integer.valueOf(((Long) row[0]).intValue()));
			}
		}
	}

	/**
	 * Take out what an earlier run copied and this one leaves behind. Raising the
	 * threshold, or measuring a duration again, changes what belongs in PostgreSQL,
	 * and a print that no longer belongs there is an answer waiting to be given
	 * wrongly. Only the files that are actually present are deleted, so the usual
	 * run costs one lookup in the table of audio files rather than a walk over
	 * every fingerprint.
	 */
	private void removeLeftBehind(Connection connection) {
		findShortResources();
		if (skipped.isEmpty())
			return;

		StringBuilder ids = new StringBuilder();
		for (Integer resourceID : skipped) {
			if (ids.length() > 0)
				ids.append(',');
			ids.append(resourceID.longValue());
		}

		List<Long> present = new ArrayList<Long>();
		try (Statement statement = connection.createStatement();
				ResultSet result = statement.executeQuery(
						"SELECT resource_id FROM panako_resource WHERE resource_id IN (" + ids + ")")) {
			while (result.next()) {
				present.add(Long.valueOf(result.getLong(1)));
			}
		} catch (SQLException e) {
			throw new RuntimeException("Could not look for audio files that are no longer copied", e);
		}
		if (present.isEmpty())
			return;

		StringBuilder remove = new StringBuilder();
		for (Long resourceID : present) {
			if (remove.length() > 0)
				remove.append(',');
			remove.append(resourceID.longValue());
		}
		try {
			connection.setAutoCommit(false);
			try (Statement statement = connection.createStatement()) {
				int fingerprints = statement
						.executeUpdate("DELETE FROM panako_fingerprint WHERE resource_id IN (" + remove + ")");
				statement.executeUpdate("DELETE FROM panako_resource WHERE resource_id IN (" + remove + ")");
				connection.commit();
				System.out.printf("> %d audio files copied earlier are left behind now: %d fingerprints removed\n",
						present.size(), fingerprints);
			}
		} catch (SQLException e) {
			try {
				connection.rollback();
			} catch (SQLException rollbackFailure) {
				LOG.warning("Could not roll back a removal: " + rollbackFailure.getMessage());
			}
			throw new RuntimeException("Could not remove the audio files that are no longer copied", e);
		} finally {
			try {
				connection.setAutoCommit(true);
			} catch (SQLException e) {
				LOG.warning("Could not restore auto commit: " + e.getMessage());
			}
		}
	}

	/**
	 * Write the files that are left behind to their own table, so whoever fetches
	 * audio can be handed the list rather than having to guess at it.
	 */
	private void recordShortResources(Connection connection) {
		findShortResources();
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TABLE IF NOT EXISTS panako_migration_short_resource ("
					+ "resource_id bigint PRIMARY KEY,"
					+ "path text NOT NULL,"
					+ "duration real NOT NULL,"
					+ "fingerprints integer NOT NULL)");
		} catch (SQLException e) {
			throw new RuntimeException("Could not create the table of short audio files", e);
		}
		// The list answers "what is worth fetching in full" now, rather than
		// logging everything ever left behind: a file that no longer qualifies -
		// because the threshold moved, or because its duration was measured again -
		// has to leave it, or someone is sent after a record already held in full.
		try (Statement statement = connection.createStatement()) {
			if (skipped.isEmpty()) {
				statement.executeUpdate("DELETE FROM panako_migration_short_resource");
			} else {
				StringBuilder keep = new StringBuilder();
				for (Integer resourceID : skipped) {
					if (keep.length() > 0)
						keep.append(',');
					keep.append(resourceID.longValue());
				}
				statement.executeUpdate(
						"DELETE FROM panako_migration_short_resource WHERE resource_id NOT IN (" + keep + ")");
			}
		} catch (SQLException e) {
			throw new RuntimeException("Could not remove the audio files that are no longer left behind", e);
		}

		if (skipped.isEmpty())
			return;

		String sql = "INSERT INTO panako_migration_short_resource (resource_id,path,duration,fingerprints) "
				+ "VALUES (?,?,?,?) ON CONFLICT (resource_id) DO UPDATE SET path = EXCLUDED.path, "
				+ "duration = EXCLUDED.duration, fingerprints = EXCLUDED.fingerprints";
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			for (Object[] row : readMetadata()) {
				if (!skipped.contains(Integer.valueOf(((Long) row[0]).intValue())))
					continue;
				statement.setLong(1, (Long) row[0]);
				statement.setString(2, (String) row[1]);
				statement.setFloat(3, (Float) row[2]);
				statement.setInt(4, (Integer) row[3]);
				statement.addBatch();
			}
			statement.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException("Could not record the short audio files", e);
		}
		System.out.printf("> %d audio files last less than %.0f seconds: left behind, listed to fetch in full\n",
				skipped.size(), minimumDuration);
	}

	private void copyFingerprints(Connection connection, Long resumeAt) {
		createStagingTable(connection);

		if (resumeAt != null) {
			System.out.printf("> Resuming at hash %d\n", resumeAt);
		}

		long copied = 0;
		long start = System.currentTimeMillis();
		while (true) {
			Batch batch = readBatch(resumeAt);
			if (batch.lastHash == null) {
				break;
			}
			writeBatch(connection, batch);
			copied += batch.rows;
			resumeAt = batch.lastHash;

			double seconds = (System.currentTimeMillis() - start) / 1000.0;
			System.out.printf("> %d fingerprints copied, %.0f/s, at hash %d\n",
					copied, seconds > 0 ? copied / seconds : 0, batch.lastHash);

			if (batch.exhausted) {
				break;
			}
		}
		System.out.printf("> Done, %d fingerprints copied in this run\n", copied);
	}

	/**
	 * Read one bounded batch of fingerprints, in a read transaction that is closed
	 * before anything is written to PostgreSQL.
	 */
	private Batch readBatch(Long resumeAt) {
		Batch batch = new Batch();
		StringBuilder rows = new StringBuilder(24 * (int) Math.min(batchSize, 1_000_000));
		ByteBuffer keyBuffer = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);

		try (Txn<ByteBuffer> txn = source.env.txnRead()) {
			Cursor<ByteBuffer> cursor = source.fingerprints.openCursor(txn);
			boolean positioned;
			if (resumeAt == null) {
				positioned = cursor.seek(SeekOp.MDB_FIRST);
			} else {
				keyBuffer.putLong(resumeAt).flip();
				positioned = cursor.get(keyBuffer, GetOp.MDB_SET_RANGE);
			}

			boolean haveHash = false;
			long previousHash = 0;
			// Entries read rather than rows kept: a batch of nothing but skipped files
			// still has to end, or one read transaction would cover the whole store.
			long scanned = 0;
			while (positioned) {
				long hash = cursor.key().order(ByteOrder.LITTLE_ENDIAN).getLong();
				// A batch ends where a hash ends. All the fingerprints of one hash sit
				// together, so stopping between two of them would need a finer resume
				// point than a hash; stopping after the last one does not.
				if (haveHash && hash != previousHash && scanned >= batchSize) {
					break;
				}
				scanned++;
				ByteBuffer value = cursor.val();
				int resourceID = value.getInt();
				int t = value.getInt();
				int f = value.getInt();
				if (skipped.contains(resourceID)) {
					// Still a hash that was read, so the batch may end here: the resume
					// point is a hash, not a row.
					previousHash = hash;
					haveHash = true;
					positioned = cursor.seek(SeekOp.MDB_NEXT);
					continue;
				}

				rows.append(hash).append('\t').append(resourceID).append('\t')
						.append(t).append('\t').append(f).append('\n');
				batch.rows++;
				previousHash = hash;
				haveHash = true;

				positioned = cursor.seek(SeekOp.MDB_NEXT);
			}
			batch.exhausted = !positioned;
			batch.lastHash = haveHash ? previousHash : null;
			cursor.close();
		}

		batch.data = rows.toString().getBytes(StandardCharsets.UTF_8);
		return batch;
	}

	/**
	 * Write one batch and its resume point in a single transaction: progress can
	 * never claim more than what is actually stored, nor less.
	 */
	private void writeBatch(Connection connection, Batch batch) {
		try {
			connection.setAutoCommit(false);
			try (Statement statement = connection.createStatement()) {
				statement.execute("TRUNCATE panako_migration_staging");
			}
			CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
			copyManager.copyIn("COPY panako_migration_staging (hash,resource_id,t,f) FROM STDIN",
					new ByteArrayInputStream(batch.data));
			try (Statement statement = connection.createStatement()) {
				statement.executeUpdate("INSERT INTO panako_fingerprint (hash,resource_id,t,f) "
						+ "SELECT hash,resource_id,t,f FROM panako_migration_staging ON CONFLICT DO NOTHING");
			}
			try (PreparedStatement statement = connection.prepareStatement(
					"INSERT INTO panako_migration_progress (id,last_hash,fingerprints,source,minimum_duration) "
					+ "VALUES (1,?,?,?,?) "
					+ "ON CONFLICT (id) DO UPDATE SET last_hash = EXCLUDED.last_hash, "
					+ "fingerprints = panako_migration_progress.fingerprints + EXCLUDED.fingerprints, "
					+ "source = EXCLUDED.source, minimum_duration = EXCLUDED.minimum_duration, "
					+ "updated_at = now()")) {
				statement.setLong(1, batch.lastHash);
				statement.setLong(2, batch.rows);
				statement.setString(3, sourceName());
				statement.setFloat(4, minimumDuration);
				statement.executeUpdate();
			}
			connection.commit();
		} catch (SQLException | IOException e) {
			try {
				connection.rollback();
			} catch (SQLException rollbackFailure) {
				LOG.warning("Could not roll back a migration batch: " + rollbackFailure.getMessage());
			}
			throw new RuntimeException("Could not copy a batch of fingerprints", e);
		} finally {
			try {
				connection.setAutoCommit(true);
			} catch (SQLException e) {
				LOG.warning("Could not restore auto commit: " + e.getMessage());
			}
		}
	}

	/**
	 * Copy the meta-data of every stored file. There are as many rows here as
	 * there are audio files, so this is done in full on every run: it costs
	 * seconds and it keeps the meta-data of files stored since the last run up to
	 * date.
	 */
	private long copyMetadata(Connection connection) {
		List<Object[]> rows = new ArrayList<Object[]>();
		for (Object[] row : readMetadata()) {
			if (!skipped.contains(Integer.valueOf(((Long) row[0]).intValue()))) {
				rows.add(row);
			}
		}

		String sql = "INSERT INTO panako_resource (resource_id,path,duration,fingerprints) VALUES (?,?,?,?) "
				+ "ON CONFLICT (resource_id) DO UPDATE SET path = EXCLUDED.path, "
				+ "duration = EXCLUDED.duration, fingerprints = EXCLUDED.fingerprints";
		try {
			connection.setAutoCommit(false);
			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				for (Object[] row : rows) {
					statement.setLong(1, (Long) row[0]);
					statement.setString(2, (String) row[1]);
					statement.setFloat(3, (Float) row[2]);
					statement.setInt(4, (Integer) row[3]);
					statement.addBatch();
				}
				statement.executeBatch();
			}
			connection.commit();
		} catch (SQLException e) {
			try {
				connection.rollback();
			} catch (SQLException rollbackFailure) {
				LOG.warning("Could not roll back the meta-data copy: " + rollbackFailure.getMessage());
			}
			throw new RuntimeException("Could not copy the meta-data", e);
		} finally {
			try {
				connection.setAutoCommit(true);
			} catch (SQLException e) {
				LOG.warning("Could not restore auto commit: " + e.getMessage());
			}
		}
		return rows.size();
	}

	/**
	 * The meta-data of every stored file: identifier, path, duration and the number
	 * of fingerprints.
	 */
	private List<Object[]> readMetadata() {
		List<Object[]> rows = new ArrayList<Object[]>();
		try (Txn<ByteBuffer> txn = source.env.txnRead();
				CursorIterable<ByteBuffer> iterable = source.resourceMap.iterate(txn)) {
			for (KeyVal<ByteBuffer> keyVal : iterable) {
				// Big endian, unlike a fingerprint hash: the two databases were written
				// with differently ordered buffers, meta-data with a plain one and
				// fingerprints with a little endian one. Read each the way it was
				// written or the identifier comes out as nonsense.
				long resourceID = keyVal.key().getLong();
				ByteBuffer value = keyVal.val();
				float duration = value.getFloat();
				int fingerprints = value.getInt();
				byte[] pathBytes = new byte[value.remaining()];
				value.get(pathBytes);
				String path = new String(pathBytes, StandardCharsets.UTF_8);
				rows.add(new Object[] { resourceID, path, duration, fingerprints });
			}
		}
		return rows;
	}

	private int compareMetadata(Connection connection) {
		int differences = 0;
		try (PreparedStatement statement = connection.prepareStatement(
				"SELECT path,duration,fingerprints FROM panako_resource WHERE resource_id = ?")) {
			for (Object[] row : readMetadata()) {
				if (skipped.contains(Integer.valueOf(((Long) row[0]).intValue())))
					continue;
				statement.setLong(1, (Long) row[0]);
				try (ResultSet result = statement.executeQuery()) {
					boolean same = result.next()
							&& result.getString(1).equals(row[1])
							&& result.getFloat(2) == ((Float) row[2]).floatValue()
							&& result.getInt(3) == ((Integer) row[3]).intValue();
					if (!same) {
						differences++;
						if (differences <= 5) {
							System.out.printf("> meta-data differs for resource %d (%s)\n", row[0], row[1]);
						}
					}
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Could not compare meta-data", e);
		}
		return differences;
	}

	private void createProgressTable(Connection connection) {
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TABLE IF NOT EXISTS panako_migration_progress ("
					+ "id integer PRIMARY KEY,"
					+ "last_hash bigint NOT NULL,"
					+ "fingerprints bigint NOT NULL,"
					+ "updated_at timestamptz NOT NULL DEFAULT now())");
			// A resume point is a hash of one store copied under one threshold. Both
			// are kept beside it, so a run against another store, or with another
			// threshold, cannot quietly continue from it.
			statement.execute("ALTER TABLE panako_migration_progress "
					+ "ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT ''");
			statement.execute("ALTER TABLE panako_migration_progress "
					+ "ADD COLUMN IF NOT EXISTS minimum_duration real NOT NULL DEFAULT 0");
		} catch (SQLException e) {
			throw new RuntimeException("Could not create the migration progress table", e);
		}
	}

	/** The store being read, as it is configured, to hold a resume point to. */
	private String sourceName() {
		return Config.get(Key.PANAKO_LMDB_FOLDER);
	}

	private void createStagingTable(Connection connection) {
		try (Statement statement = connection.createStatement()) {
			// Unlogged: its contents live for the length of one batch and are already
			// safe in LMDB, so there is no reason to write them to the write ahead log
			// as well.
			statement.execute("CREATE UNLOGGED TABLE IF NOT EXISTS panako_migration_staging ("
					+ "hash bigint NOT NULL,"
					+ "resource_id integer NOT NULL,"
					+ "t integer NOT NULL,"
					+ "f integer NOT NULL)");
		} catch (SQLException e) {
			throw new RuntimeException("Could not create the migration staging table", e);
		}
	}

	private Long readProgress(Connection connection) {
		try (Statement statement = connection.createStatement();
				ResultSet result = statement.executeQuery(
						"SELECT last_hash,source,minimum_duration FROM panako_migration_progress WHERE id = 1")) {
			if (!result.next())
				return null;
			long lastHash = result.getLong(1);
			String progressSource = result.getString(2);
			float progressDuration = result.getFloat(3);
			boolean sameSource = progressSource == null || progressSource.isEmpty()
					|| progressSource.equals(sourceName());
			if (!sameSource || progressDuration != minimumDuration) {
				throw new IllegalStateException(String.format(
						"The progress in this database was made copying '%s' with a minimum duration of %.1f "
								+ "seconds, and this run reads '%s' with %.1f. Continuing from it would leave "
								+ "fingerprints behind, or keep ones that are now left behind: run "
								+ "'panako migrate --restart' to start over.",
						progressSource, progressDuration, sourceName(), minimumDuration));
			}
			return Long.valueOf(lastHash);
		} catch (SQLException e) {
			throw new RuntimeException("Could not read the migration progress", e);
		}
	}

	private long count(Connection connection, String sql) {
		try (Statement statement = connection.createStatement();
				ResultSet result = statement.executeQuery(sql)) {
			return result.next() ? result.getLong(1) : 0;
		} catch (SQLException e) {
			throw new RuntimeException("Could not count rows: " + sql, e);
		}
	}

	private static class Batch {
		private long rows;
		private Long lastHash;
		private boolean exhausted;
		private byte[] data;
	}
}
