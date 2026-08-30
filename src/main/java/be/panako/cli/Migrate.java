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

import be.panako.strategy.panako.storage.PanakoStorageKV;
import be.panako.strategy.panako.storage.PanakoStorageMigration;
import be.panako.strategy.panako.storage.PanakoStoragePostgres;

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
			} else {
				System.err.println("Unknown argument '" + argument + "'");
				printHelp();
				return;
			}
		}

		PanakoStorageKV lmdb = PanakoStorageKV.getInstance();
		PanakoStoragePostgres postgres = PanakoStoragePostgres.getInstance();
		PanakoStorageMigration migration = new PanakoStorageMigration(lmdb, postgres, batchSize);

		if (restart) {
			migration.restart();
		}

		if (!verifyOnly) {
			migration.migrate();
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

	@Override
	public String description() {
		return "Copies the fingerprints in the LMDB store into the PostgreSQL store.\n"
				+ "\tThe LMDB store is only read and stays usable throughout. The copy can be\n"
				+ "\tinterrupted and resumes where it stopped.";
	}

	@Override
	public String synopsis() {
		return "migrate [--verify] [--no-verify] [--restart] [--batch=500000] [--sample=1000]";
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
