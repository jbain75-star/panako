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
 *
 * <p>
 * A run copies the files the source held when it started, and no others. The
 * source is live: files are stored in it for as long as the copy runs, and one
 * stored below the hash the copy has already passed would never be met. Holding
 * a run to the files it began with is what makes "both stores hold the same
 * thing" a question with an answer; the alternative is a target that moves as
 * fast as it is approached and a copy that can never be called finished. The
 * identifiers are written down in {@code panako_migration_run_resource} when a
 * walk begins and kept while it resumes, and the next walk takes them again:
 * that next walk is the one that copies whatever arrived during this one.
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

	/**
	 * Written beside the identifiers a run is copying, so that a run beginning
	 * while the source is empty is still a run that was written down. Resource
	 * identifiers are never negative.
	 */
	private static final long MARK = -1;

	private final PanakoStorageKV source;
	private final PanakoStoragePostgres target;
	private final long batchSize;
	private final float minimumDuration;

	/** Resource identifiers left behind, empty when nothing is left behind. */
	private Set<Integer> skipped = new HashSet<Integer>();

	/**
	 * The files this run copies: what the source held when the walk began. Null
	 * until it is read or written, and null for a store copied before runs were
	 * written down, where every file the source holds is the answer.
	 */
	private Set<Integer> runResources = null;

	/**
	 * Files whose length was never recorded. They are copied - a length that was
	 * never measured is not a short length - and counted, because a file that is
	 * only thirty seconds long and does not say so is copied along with them.
	 */
	private long unmeasured = 0;

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
		createRunTable(connection);
		// Read before anything is written: a resume point that belongs to another
		// store or another threshold has to stop the run while PostgreSQL still
		// holds what the earlier run left there.
		Long resumeAt = readProgress(connection);
		if (resumeAt != null) {
			// A walk that resumes keeps the list the interrupted one was working
			// from, or it would be asked to have copied files it never saw.
			readRunResources(connection);
			if (runResources == null) {
				// A resume point left by a run made before runs were written down:
				// what it was copying cannot be told from what has been stored since.
				// The walk starts again from the first hash instead - fingerprints
				// already copied conflict and are skipped, and a second walk costs a
				// fraction of the first.
				System.out.println("> The earlier run left no record of what it was copying: starting from the "
						+ "first hash, fingerprints already copied are skipped");
				resumeAt = null;
			}
		}
		if (resumeAt == null) {
			// A walk that starts at the first hash starts a new run, and takes down
			// the files it is going to copy.
			recordRunResources(connection);
		}
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
		createRunTable(connection);
		try (Statement statement = connection.createStatement()) {
			statement.execute("DELETE FROM panako_migration_progress");
			statement.execute("TRUNCATE panako_migration_run_resource");
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
		createRunTable(connection);
		readRunResources(connection);
		findShortResources();

		long pgFingerprints = count(connection, "SELECT count(*) FROM panako_fingerprint");
		long pgResources = count(connection, "SELECT count(*) FROM panako_resource");

		// One walk over the fingerprints, counting each entry against the run it
		// belongs to: what this run had to copy, what it deliberately left behind,
		// and what was stored after it began. A second walk to count the same
		// entries differently would cost as much as the first.
		Tally tally = tally();
		Tally resources = tallyResources();

		System.out.printf("> audio files:   lmdb %d, postgres %d\n", resources.expected, pgResources);
		System.out.printf("> fingerprints:  lmdb %d, postgres %d\n", tally.expected, pgFingerprints);
		if (!skipped.isEmpty()) {
			System.out.printf("> left behind:   %d audio files shorter than %.0f seconds\n",
					skipped.size(), minimumDuration);
		}
		// Printed whether or not there is any, and in one line the caller can read
		// without arithmetic: "the copy is complete and N files have been stored
		// since" is the normal state of a copy made next to a store that is still
		// being written to, and it is not a failure.
		System.out.printf("> filed since:   %d audio files, %d fingerprints\n",
				resources.arrived, tally.arrived);
		if (unmeasured > 0) {
			System.out.printf("> no length:     %d audio files have no recorded length and are copied\n", unmeasured);
		}

		// Meta-data is what turns a match into a file name, so every row of it is
		// compared rather than sampled: there are as many as there are audio files.
		int metadataDifferences = compareMetadata(connection);
		System.out.printf("> meta-data:     %d of %d audio files differ\n", metadataDifferences, resources.expected);

		int checked = 0;
		int missing = 0;
		if (sampleSize > 0 && tally.expected > 0) {
			// Every nth fingerprint rather than the first n: the copy walks the hash
			// space in order, so a sample taken from the start would pass while a
			// half finished copy was missing everything above some hash. The stride
			// counts only what should have been copied, so leaving files behind
			// thins the sample rather than the coverage.
			long step = Math.max(1, tally.expected / sampleSize);
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
					if (!copies(resourceID))
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
		// in PostgreSQL is a print of something the run was not asked to copy - a
		// stale answer waiting to be given. What arrived after the run began is
		// counted, printed and then left out of the comparison entirely: it is the
		// next run's work, not this run's failure.
		boolean equal = resources.expected == pgResources && tally.expected == pgFingerprints && missing == 0
				&& metadataDifferences == 0;
		System.out.println(equal ? "> the stores agree" : "> the stores DO NOT agree");
		if (equal && resources.arrived > 0) {
			System.out.printf("> %d audio files have been stored since this copy began: run the copy again to "
					+ "take them across\n", resources.arrived);
		}
		return equal;
	}

	/** What one walk over the fingerprints in LMDB adds up to. */
	private static class Tally {
		/** Entries belonging to a file this run had to copy. */
		private long expected = 0;
		/** Entries belonging to a file stored after this run began. */
		private long arrived = 0;
	}

	/**
	 * Count the fingerprints in LMDB, apart into what this run had to copy and what
	 * was stored after it began. Counted rather than added up from the meta-data:
	 * the meta-data records how many prints were extracted from a file, which is
	 * not how many entries they occupy once identical ones have collapsed.
	 */
	private Tally tally() {
		Tally tally = new Tally();
		try (Txn<ByteBuffer> txn = source.env.txnRead();
				CursorIterable<ByteBuffer> iterable = source.fingerprints.iterate(txn)) {
			for (KeyVal<ByteBuffer> keyVal : iterable) {
				int resourceID = keyVal.val().getInt();
				if (copies(resourceID)) {
					tally.expected++;
				} else if (!skipped.contains(Integer.valueOf(resourceID))) {
					tally.arrived++;
				}
			}
		}
		return tally;
	}

	/**
	 * Whether a file belongs to this run: it is not left behind for being short,
	 * and it was in the source when the walk began. A store copied before runs were
	 * written down has no such list, and then every file that is not left behind
	 * belongs to the run.
	 */
	private boolean copies(int resourceID) {
		Integer id = Integer.valueOf(resourceID);
		if (skipped.contains(id))
			return false;
		return runResources == null || runResources.contains(id);
	}

	/**
	 * Count the audio files in LMDB the same way as the fingerprints: what this run
	 * had to copy, and what was stored after it began. A file this run was to copy
	 * that the source no longer holds is not expected of PostgreSQL either - it is
	 * counted where it is, and if PostgreSQL still holds it the stores disagree,
	 * which is the truth: it is a name waiting to be given to a file that is gone.
	 */
	private Tally tallyResources() {
		Tally tally = new Tally();
		for (Object[] row : readMetadata()) {
			Integer resourceID = Integer.valueOf(((Long) row[0]).intValue());
			if (skipped.contains(resourceID))
				continue;
			if (runResources == null || runResources.contains(resourceID)) {
				tally.expected++;
			} else {
				tally.arrived++;
			}
		}
		return tally;
	}

	/**
	 * Fill {@link #skipped} with the files that last less than the minimum
	 * duration. A duration of zero or less is unknown rather than short, so such a
	 * file is copied: leaving it behind on a number that was never measured would
	 * throw away a full print.
	 */
	private void findShortResources() {
		skipped = new HashSet<Integer>();
		unmeasured = 0;
		if (minimumDuration <= 0)
			return;
		for (Object[] row : readMetadata()) {
			float duration = ((Float) row[2]).floatValue();
			if (duration > 0 && duration < minimumDuration) {
				skipped.add(Integer.valueOf(((Long) row[0]).intValue()));
			} else if (duration <= 0) {
				unmeasured++;
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
		if (unmeasured > 0) {
			System.out.printf("> %d audio files have no recorded length: copied, a length never measured is not "
					+ "a short one\n", unmeasured);
		}
	}

	private void copyFingerprints(Connection connection, Long resumeAt) {
		createStagingTable(connection);

		if (resumeAt != null) {
			System.out.printf("> Resuming at hash %d\n", resumeAt);
		}

		// The hash the walk ends at, said out loud once: how far along a run is is
		// how far through the hashes it has walked, and counting fingerprints
		// written cannot say that when most of what is walked is already there or
		// left behind on purpose.
		Long lastHash = lastHash();
		if (lastHash != null) {
			System.out.printf("> The walk ends at hash %d\n", lastHash);
		}

		long copied = 0;
		long start = System.currentTimeMillis();
		while (true) {
			Batch batch = readBatch(resumeAt);
			if (batch.lastHash == null) {
				break;
			}
			writeBatch(connection, batch);
			copied += batch.inserted;
			resumeAt = batch.lastHash;

			double seconds = (System.currentTimeMillis() - start) / 1000.0;
			System.out.printf("> %d fingerprints copied, %.0f/s, at hash %d\n",
					copied, seconds > 0 ? copied / seconds : 0, batch.lastHash);

			if (batch.exhausted) {
				// The walk has met the last hash, so this run has copied everything it
				// was given. Writing that down is what lets the next run be a new run
				// rather than the resumption of a finished one, and only a new run
				// takes across what was stored while this one ran: a file stored below
				// the hash a walk has already passed is never met by that walk.
				completeRun(connection);
				break;
			}
		}
		System.out.printf("> Done, %d fingerprints copied in this run\n", copied);
	}

	/**
	 * The largest hash the store holds, or null for a store with no fingerprints
	 * in it. Hashes are stored as integer keys, so the last key is the largest.
	 */
	private Long lastHash() {
		try (Txn<ByteBuffer> txn = source.env.txnRead()) {
			Cursor<ByteBuffer> cursor = source.fingerprints.openCursor(txn);
			try {
				if (!cursor.seek(SeekOp.MDB_LAST))
					return null;
				return Long.valueOf(cursor.key().order(ByteOrder.LITTLE_ENDIAN).getLong());
			} finally {
				cursor.close();
			}
		}
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
				if (!copies(resourceID)) {
					// Left behind for being short, or stored after this run began.
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
			if (batch.rows > 0) {
				CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
				copyManager.copyIn("COPY panako_migration_staging (hash,resource_id,t,f) FROM STDIN",
						new ByteArrayInputStream(batch.data));
				try (Statement statement = connection.createStatement()) {
					// Rows that were actually added, not rows that were offered: a walk
					// that starts again meets everything already copied, and counting
					// those again would have progress climb past the number of
					// fingerprints there are.
					batch.inserted = statement.executeUpdate("INSERT INTO panako_fingerprint (hash,resource_id,t,f) "
							+ "SELECT hash,resource_id,t,f FROM panako_migration_staging ON CONFLICT DO NOTHING");
				}
			}
			try (PreparedStatement statement = connection.prepareStatement(
					"INSERT INTO panako_migration_progress (id,last_hash,fingerprints,source,minimum_duration) "
					+ "VALUES (1,?,?,?,?) "
					+ "ON CONFLICT (id) DO UPDATE SET last_hash = EXCLUDED.last_hash, "
					+ "fingerprints = panako_migration_progress.fingerprints + EXCLUDED.fingerprints, "
					+ "source = EXCLUDED.source, minimum_duration = EXCLUDED.minimum_duration, "
					+ "completed = false, updated_at = now()")) {
				statement.setLong(1, batch.lastHash);
				statement.setLong(2, batch.inserted);
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
			if (copies(((Long) row[0]).intValue())) {
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
				if (!copies(((Long) row[0]).intValue()))
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
			// Whether the walk this resume point belongs to has met the last hash. A
			// finished walk is not resumed: continuing from its last hash would copy
			// nothing for ever, while the files stored since it began sit below that
			// hash, waiting for a walk that starts at the first one.
			statement.execute("ALTER TABLE panako_migration_progress "
					+ "ADD COLUMN IF NOT EXISTS completed boolean NOT NULL DEFAULT false");
		} catch (SQLException e) {
			throw new RuntimeException("Could not create the migration progress table", e);
		}
	}

	/**
	 * The table holding the files a run is copying: the contents of the source when
	 * the walk began.
	 */
	private void createRunTable(Connection connection) {
		try (Statement statement = connection.createStatement()) {
			statement.execute("CREATE TABLE IF NOT EXISTS panako_migration_run_resource ("
					+ "resource_id bigint PRIMARY KEY)");
		} catch (SQLException e) {
			throw new RuntimeException("Could not create the table of the files being copied", e);
		}
	}

	/**
	 * Write down the files the source holds now: the run being started copies these
	 * and no others. Anything stored from this moment on is the next run's work.
	 */
	private void recordRunResources(Connection connection) {
		Set<Integer> resources = new HashSet<Integer>();
		for (Object[] row : readMetadata()) {
			resources.add(Integer.valueOf(((Long) row[0]).intValue()));
		}
		try {
			connection.setAutoCommit(false);
			try (Statement statement = connection.createStatement()) {
				statement.execute("TRUNCATE panako_migration_run_resource");
				// A run over an empty source has to be readable as a run all the same,
				// or an arrival before it verifies is taken for work it never did.
				statement.execute("INSERT INTO panako_migration_run_resource (resource_id) VALUES (" + MARK + ")");
			}
			try (PreparedStatement statement = connection
					.prepareStatement("INSERT INTO panako_migration_run_resource (resource_id) VALUES (?)")) {
				for (Integer resourceID : resources) {
					statement.setLong(1, resourceID.longValue());
					statement.addBatch();
				}
				statement.executeBatch();
			}
			connection.commit();
		} catch (SQLException e) {
			try {
				connection.rollback();
			} catch (SQLException rollbackFailure) {
				LOG.warning("Could not roll back the list of files to copy: " + rollbackFailure.getMessage());
			}
			throw new RuntimeException("Could not write down the files this run copies", e);
		} finally {
			try {
				connection.setAutoCommit(true);
			} catch (SQLException e) {
				LOG.warning("Could not restore auto commit: " + e.getMessage());
			}
		}
		runResources = resources;
		System.out.printf("> This run copies the %d audio files the store holds now\n", resources.size());
	}

	/**
	 * Read the files the run in progress is copying, leaving {@link #runResources}
	 * null only when no run was written down at all: a store copied before runs
	 * were written down, where the source as it is now is the only list there is. A
	 * run that began while the source was empty has an empty list, which is a
	 * different thing and is why the mark is written beside the identifiers.
	 */
	private void readRunResources(Connection connection) {
		Set<Integer> resources = new HashSet<Integer>();
		boolean recorded = false;
		try (Statement statement = connection.createStatement();
				ResultSet result = statement.executeQuery("SELECT resource_id FROM panako_migration_run_resource")) {
			while (result.next()) {
				long resourceID = result.getLong(1);
				if (resourceID == MARK) {
					recorded = true;
				} else {
					resources.add(Integer.valueOf((int) resourceID));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Could not read the files this run copies", e);
		}
		runResources = recorded || !resources.isEmpty() ? resources : null;
	}

	/** Write down that the walk has met the last hash. */
	private void completeRun(Connection connection) {
		try (Statement statement = connection.createStatement()) {
			statement.executeUpdate("UPDATE panako_migration_progress SET completed = true, updated_at = now() "
					+ "WHERE id = 1");
		} catch (SQLException e) {
			throw new RuntimeException("Could not record that the copy reached the last hash", e);
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
				ResultSet result = statement.executeQuery("SELECT last_hash,source,minimum_duration,completed "
						+ "FROM panako_migration_progress WHERE id = 1")) {
			if (!result.next())
				return null;
			long lastHash = result.getLong(1);
			String progressSource = result.getString(2);
			float progressDuration = result.getFloat(3);
			if (result.getBoolean(4)) {
				// The last walk finished. There is nothing above its last hash, and
				// what has been stored since is below it: this is a new run, over
				// everything, and the fingerprints already copied cost one conflict
				// each. It is also why a threshold that has moved needs no complaint
				// here - nothing is being continued.
				System.out.println("> The last copy finished: this run starts again at the first hash and takes "
						+ "across what has been stored since");
				return null;
			}
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
		private long inserted;
		private Long lastHash;
		private boolean exhausted;
		private byte[] data;
	}
}
