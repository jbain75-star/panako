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

package be.panako.cli;

import java.io.File;

import be.panako.strategy.panako.storage.PanakoStorageKV;
import be.panako.strategy.panako.storage.PanakoStorageMigration;
import be.panako.strategy.panako.storage.PanakoStoragePostgres;
import be.panako.util.Config;
import be.panako.util.FileUtils;
import be.panako.util.Key;

/**
 * Copies the fingerprints of the LMDB store into a PostgreSQL store.
 *
 * <p>
 * Both stores are opened directly, whatever {@code PANAKO_STORAGE} says: the
 * point is to read the one and fill the other, while the LMDB store keeps
 * answering queries for everyone else. LMDB is only read.
 * </p>
 */
class Migrate extends Application {

	@Override
	public void run(String... args) {
		boolean verifyOnly = false;
		boolean verifyAfter = true;
		boolean restart = false;
		long batchSize = PanakoStorageMigration.DEFAULT_BATCH_SIZE;
		int sampleSize = 1000;
		float minimumDuration = 0;

		for (String argument : args) {
			if (argument.equals("--verify")) {
				verifyOnly = true;
			} else if (argument.equals("--restart")) {
				restart = true;
			} else if (argument.equals("--no-verify")) {
				verifyAfter = false;
			} else if (argument.startsWith("--batch=")) {
				batchSize = Long.parseLong(argument.substring("--batch=".length()));
			} else if (argument.startsWith("--sample=")) {
				sampleSize = Integer.parseInt(argument.substring("--sample=".length()));
			} else if (argument.startsWith("--min-duration=")) {
				minimumDuration = Float.parseFloat(argument.substring("--min-duration=".length()));
			} else {
				System.err.println("Unknown argument '" + argument + "'");
				printHelp();
				return;
			}
		}

		if (!sourceStoreExists()) {
			// An LMDB store is created on demand, so a wrong or missing folder would
			// otherwise read as a store with nothing in it: the copy would report
			// success having moved nothing, and verification would agree.
			System.err.println("There is no LMDB fingerprint store at "
					+ FileUtils.expandHomeDir(Config.get(Key.PANAKO_LMDB_FOLDER))
					+ ". Set PANAKO_LMDB_FOLDER to the store to copy.");
			System.exit(1);
			return;
		}

		PanakoStorageKV lmdb = PanakoStorageKV.getInstance();
		PanakoStoragePostgres postgres = PanakoStoragePostgres.getInstance();
		PanakoStorageMigration migration = new PanakoStorageMigration(lmdb, postgres, batchSize, minimumDuration);

		if (restart) {
			migration.restart();
		}

		if (!verifyOnly) {
			try {
				migration.migrate();
			} catch (IllegalStateException e) {
				// A refusal to resume is an answer, not a crash: it is worth reading
				// rather than being buried under a stack trace.
				System.err.println(e.getMessage());
				System.exit(1);
				return;
			}
		}

		if (verifyOnly || verifyAfter) {
			boolean equal = migration.verify(sampleSize);
			if (!equal) {
				// A caller running this unattended needs to know without reading the
				// output, and a store that is not equal yet is not a store to switch to.
				System.exit(1);
			}
		}
	}

	/**
	 * Whether the configured LMDB folder actually holds a store: the data file
	 * LMDB keeps everything in has to be there already.
	 */
	private boolean sourceStoreExists() {
		File folder = new File(FileUtils.expandHomeDir(Config.get(Key.PANAKO_LMDB_FOLDER)));
		return folder.isDirectory() && new File(folder, "data.mdb").exists();
	}

	@Override
	public String description() {
		return "Copies the fingerprints in the LMDB store into the PostgreSQL store.\n"
				+ "\tThe LMDB store is only read and stays usable throughout. The copy can be\n"
				+ "\tinterrupted and resumes where it stopped.\n"
				+ "\t--min-duration leaves the prints of files shorter than that many seconds\n"
				+ "\tbehind, and lists them in panako_migration_short_resource instead.";
	}

	@Override
	public String synopsis() {
		return "migrate [--verify] [--no-verify] [--restart] [--batch=500000] [--sample=1000] [--min-duration=0]";
	}

	@Override
	public boolean needsStorage() {
		// Both stores are opened here, by name, rather than the one the configured
		// strategy happens to point at.
		return false;
	}

	@Override
	public boolean writesToStorage() {
		return false;
	}
}
