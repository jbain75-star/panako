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
****************************************************************************
*    ______   ________   ___   __    ________   ___   ___   ______         *
*   /_____/\ /_______/\ /__/\ /__/\ /_______/\ /___/\/__/\ /_____/\        *
*   \:::_ \ \\::: _  \ \\::\_\\  \ \\::: _  \ \\::.\ \\ \ \\:::_ \ \       *
*    \:(_) \ \\::(_)  \ \\:. `-\  \ \\::(_)  \ \\:: \/_) \ \\:\ \ \ \      *
*     \: ___\/ \:: __  \ \\:. _    \ \\:: __  \ \\:. __  ( ( \:\ \ \ \     *
*      \ \ \    \:.\ \  \ \\. \`-\  \ \\:.\ \  \ \\: \ )  \ \ \:\_\ \ \    *
*       \_\/     \__\/\__\/ \__\/ \__\/ \__\/\__\/ \__\/\__\/  \_____\/    *
*                                                                          *
****************************************************************************
*                                                                          *
*                              Panako                                      *
*                       Acoustic Fingerprinting                            *
*                                                                          *
****************************************************************************/

package be.panako.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.logging.Logger;

import be.panako.strategy.QueryResult;
import be.panako.strategy.QueryResultHandler;
import be.panako.strategy.Strategy;
import be.panako.util.Config;
import be.panako.util.Key;

/**
 * Answer queries from standard input, keeping the index open between them.
 *
 * <p>{@link Query} is a whole process per question: a JVM starts, the index is
 * opened, one file is matched, and everything is thrown away. For a batch that
 * is nothing, but for a service answering one clip at a time it is nearly all
 * of the wait — a query that takes about a second of matching took eleven from
 * the outside, and a four second clip took longer than a thirty second one,
 * because the length of the audio is not what is being paid for. This command
 * pays that cost once and then answers as long as it is fed.</p>
 *
 * <p>The conversation is line based, so it needs no library on either side:</p>
 *
 * <pre>
 *   &lt;- READY
 *   -&gt; 7\t/tmp/clip.wav
 *   &lt;- BEGIN 7
 *   &lt;- 0 ; 0 ; /tmp/clip.wav ; 0.000 ; … (a Query result row, unchanged)
 *   &lt;- END 7 ok
 *   -&gt; QUIT
 * </pre>
 *
 * <p>Result rows are printed by {@link Panako#printQueryResult} exactly as
 * {@link Query} prints them, so anything that already reads Panako's query
 * output reads this unchanged; the markers are what let a caller tell one
 * answer from the next, and tell a slow answer from a lost one. Identifiers are
 * the caller's and are echoed, never interpreted.</p>
 *
 * <p>One question at a time, in the order they arrive. A query is CPU and
 * memory hungry and the index is a memory mapped file, so several at once in
 * one process would make each slower without making the process useful to
 * anyone else; a caller that wants two at once runs two of these.</p>
 */
class Serve extends Application {
	private final static Logger LOG = Logger.getLogger(Serve.class.getName());

	@Override
	public void run(String... args) {
		final int numberOfQueryResults = Config.getInt(Key.NUMBER_OF_QUERY_RESULTS);
		// Built here, before READY is said, so the storage is open and warm by
		// the time the caller is told it may ask. A caller told READY and then
		// kept waiting while the index opened would be back to paying for the
		// startup it started this process to avoid.
		final Strategy strategy = Strategy.getInstance();
		final HashSet<Integer> emptyHashSet = new HashSet<Integer>();

		final BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
		System.out.println("READY");
		System.out.flush();

		while (true) {
			String line;
			try {
				line = in.readLine();
			} catch (IOException e) {
				// The caller went away mid-sentence. Nothing to answer to.
				LOG.info("Could not read from standard input, stopping: " + e.getMessage());
				return;
			}
			if (line == null) return;
			line = line.trim();
			if (line.isEmpty()) continue;
			if (line.equals("QUIT")) return;

			final int separator = line.indexOf('\t');
			final String id = separator < 0 ? line : line.substring(0, separator);
			final String path = separator < 0 ? "" : line.substring(separator + 1);

			System.out.println("BEGIN " + id);
			try {
				if (path.isEmpty()) throw new IllegalArgumentException("no audio file given");
				strategy.query(path, numberOfQueryResults, emptyHashSet, new PrintingHandler());
				System.out.println("END " + id + " ok");
			} catch (Throwable t) {
				// Every failure is reported and survived, including the ones a
				// batch command is right to die on: an unreadable file, audio
				// that decodes to nothing, a query that overflows. This process
				// is a service, and the next question is very likely fine — one
				// bad clip must not cost the caller its warm index, nor leave it
				// waiting on an answer that is never coming.
				System.out.println("END " + id + " error " + oneLine(t));
			}
			System.out.flush();
		}
	}

	/** Whatever went wrong, said on the single line the protocol allows. */
	private static String oneLine(Throwable t) {
		final String message = t.getMessage() == null ? "" : t.getMessage();
		final String said = t.getClass().getSimpleName() + (message.isEmpty() ? "" : ": " + message);
		return said.replace('\r', ' ').replace('\n', ' ');
	}

	private static class PrintingHandler implements QueryResultHandler {
		@Override
		public void handleQueryResult(QueryResult r) {
			Panako.printQueryResult(r, 0, 0);
		}

		@Override
		public void handleEmptyResult(QueryResult r) {
			Panako.printQueryResult(r, 0, 0);
		}
	}

	@Override
	public String description() {
		return "Answers queries read from standard input, keeping the index open between them.";
	}

	@Override
	public String synopsis() {
		return "";
	}

	@Override
	public boolean needsStorage() {
		return true;
	}

	@Override
	public boolean writesToStorage() {
		return false;
	}
}
