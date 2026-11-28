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


package be.panako.strategy.panako;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import be.panako.strategy.QueryResult;
import be.panako.strategy.QueryResultHandler;
import be.panako.strategy.Strategy;
import be.panako.strategy.panako.storage.*;
import be.panako.util.*;
import be.tarsos.dsp.AudioDispatcher;
import be.tarsos.dsp.io.jvm.AudioDispatcherFactory;
import be.tarsos.dsp.util.PitchConverter;

/**
 * The main interface to the Panako implementation
 */
public class PanakoStrategy extends Strategy {
	private static final int MAX_TIME = 5_000_000;
	
	private final static Logger LOG = Logger.getLogger(PanakoStrategy.class.getName());

	private final int latency;

	private final PanakoStorage db;

	/**
	 * Create a new instance
	 */
	public PanakoStrategy(){

		if(!Config.getBoolean(Key.PANAKO_USE_GPU_EP_EXTRACTOR)) {
			//determine the Gaborator latency to make sure the block time to seconds
			//conversion is always correct!
			int size = Config.getInt(Key.PANAKO_AUDIO_BLOCK_SIZE);
			PanakoEventPointProcessor eventPointProcessor = new PanakoEventPointProcessor(size);
			latency = eventPointProcessor.latency();
			LOG.info(String.format("Gaborator latency is %d samples", latency));
			eventPointProcessor.processingFinished();
		}else{
			latency=0;
		}

		PanakoStorage db;
		if (Config.get(Key.PANAKO_STORAGE).equalsIgnoreCase("LMDB")) {
			db = PanakoStorageKV.getInstance();
		}else if (Config.get(Key.PANAKO_STORAGE).equalsIgnoreCase("PG")) {
			db = PanakoStoragePostgres.getInstance();
		}else if (Config.get(Key.PANAKO_STORAGE).equalsIgnoreCase("FILE")) {
			db = PanakoStorageFile.getInstance();
		}else {
			db = PanakoStorageMemory.getInstance();
		}
		if(Config.getBoolean(Key.PANAKO_CACHE_TO_FILE) && db != PanakoStorageFile.getInstance()) {
			LOG.info("Using "+ db.getClass().getSimpleName() + " storage with caching front.");
			db = new PanakoCachingStorage(PanakoStorageFile.getInstance(),db);
		}else {
			LOG.info("Using " + db.getClass().getSimpleName() + " as storage.");
		}
		this.db = db;
	}

	private PanakoStorage getStorage(){
		return db;
	}
	
	@Override
	public double store(String resource, String description) {
		PanakoStorage db = getStorage();
		List<PanakoFingerprint> prints = toFingerprints(resource);
		
		int resourceID = FileUtils.getIdentifier(resource);
		//store
		for(PanakoFingerprint print : prints) {
			long hash = print.hash();
			db.addToStoreQueue(hash, resourceID, print.t1,print.f1);
		}
		db.processStoreQueue();

		//store meta-data as well
		float duration = 0;
		if(prints.size() != 0) {
			duration = blocksToSeconds(prints.get(prints.size()-1).t3);
			LOG.info(String.format("Stored %d fingerprints for '%s', id: %d", prints.size() , resource ,resourceID));
		}else {
			LOG.warning("Warning: no prints extracted for " + resource);
		}
		int numberOfPrints = prints.size();
		
		db.storeMetadata((long) resourceID,resource,duration,numberOfPrints);
		
		//storage is done: 
		//try to clear memory
		System.gc();
		
		return duration;
	}

	@Override
	public double delete(String resource) {

		PanakoStorage db = getStorage();
		
		List<PanakoFingerprint> prints = toFingerprints(resource);
		
		int resourceID = FileUtils.getIdentifier(resource);
		//delete
		for(PanakoFingerprint print : prints) {
			long hash = print.hash();
			db.addToDeleteQueue(hash, resourceID, print.t1,print.f1);
		}
		db.processDeleteQueue();
		
		//delete meta-data as well
		float duration = 0;
		if(prints.size() != 0) {
			duration = blocksToSeconds(prints.get(prints.size()-1).t3);
		}else {
			LOG.warning("Warning: no prints extracted for " + resource);
		}

		db.deleteMetadata((long) resourceID);
		
		//storage is done: 
		//try to clear memory
		System.gc();
		
		return duration;
	}
	
	private List<PanakoFingerprint> toFingerprints(String resource){
		return toFingerprints(resource,0,MAX_TIME);
	}
	
	private List<PanakoFingerprint> toFingerprints(String resource,double startTimeOffset,double numberOfSeconds){

		if(Config.getBoolean(Key.PANAKO_USE_CACHED_PRINTS)) {
			String folder = Config.get(Key.PANAKO_CACHE_FOLDER);
			folder = FileUtils.expandHomeDir(folder);
			String tdbPath =  FileUtils.combine(folder,resolve(resource) + ".tdb");

			if(FileUtils.exists(tdbPath)) {
				List<PanakoFingerprint> prints = new ArrayList<>();
				List<long[]> printData = readFingerprintFile(tdbPath);
				for(long[] data : printData) {
					long fingerprintHash = data[0];
					int t1 = (int) data[2];
					int f1 = (int) data[3];
					float t1InSeconds = blocksToSeconds(t1);

					//skip all fingerprints after stop time
					if(t1InSeconds > startTimeOffset + numberOfSeconds)
						break;
					//only add prints if they are after the start time offset
					if(t1InSeconds >= startTimeOffset)
						prints.add(new PanakoFingerprint(fingerprintHash,t1,f1));
				}
				LOG.info(String.format("Read %d cached fingerprints from file '%s' (start: %.3f sec, stop: %.3f sec) for '%s'", prints.size(),tdbPath,startTimeOffset,startTimeOffset+numberOfSeconds,resource));
				return prints;
			}else{
				LOG.info(String.format("Could not read cached fingerprints from file '%s' for '%s'",tdbPath,resource));
			}
		} //else no cached prints are found

		if(Config.getBoolean(Key.PANAKO_USE_GPU_EP_EXTRACTOR)){
			List<PanakoFingerprint> prints = new PanakoGPUEventPointProcessor().extractFingerprints(resource);
			List<PanakoFingerprint> filteredPrints = new ArrayList<>();
			for(PanakoFingerprint print : prints) {
				float t1InSeconds = blocksToSeconds(print.t1);
				//skip all fingerprints after stop time
				if(t1InSeconds > startTimeOffset + numberOfSeconds)
					break;
				//only add prints if they are after the start time offset
				if(t1InSeconds >= startTimeOffset)
					filteredPrints.add(print);
			}
			return filteredPrints;
		}else{
			int samplerate, size, overlap;
			samplerate = Config.getInt(Key.PANAKO_SAMPLE_RATE);
			size = Config.getInt(Key.PANAKO_AUDIO_BLOCK_SIZE);
			overlap = Config.getInt(Key.PANAKO_AUDIO_BLOCK_OVERLAP);

			AudioDispatcher d;

			if(numberOfSeconds==MAX_TIME)
				d = AudioDispatcherFactory.fromPipe(resource, samplerate, size, overlap,startTimeOffset);
			else
				d = AudioDispatcherFactory.fromPipe(resource, samplerate, size, overlap,startTimeOffset,numberOfSeconds);

			PanakoEventPointProcessor eventPointProcessor = new PanakoEventPointProcessor(size);
			d.addAudioProcessor(eventPointProcessor);
			d.run();

			return eventPointProcessor.getFingerprints();
		}

	}

	private float blocksToSeconds(int t) {
		float timeResolution = Config.getFloat(Key.PANAKO_TRANSF_TIME_RESOLUTION);
		float sampleRate = Config.getFloat(Key.PANAKO_SAMPLE_RATE);
		
		return t * (timeResolution/sampleRate) + latency/(float) sampleRate;
	}
	
	private static class QueryConfig {
		private final EnumMap<Key,Float> floats = new EnumMap<>(Key.class);
		private final EnumMap<Key,Integer> ints = new EnumMap<>(Key.class);

		float getFloat(Key key) {
			Float value = floats.get(key);
			if(value == null) {
				value = Config.getFloat(key);
				floats.put(key, value);
			}
			return value;
		}

		int getInt(Key key) {
			Integer value = ints.get(key);
			if(value == null) {
				value = Config.getInt(key);
				ints.put(key, value);
			}
			return value;
		}
	}

	private float blocksToSeconds(int t, QueryConfig config) {
		float timeResolution = config.getFloat(Key.PANAKO_TRANSF_TIME_RESOLUTION);
		float sampleRate = config.getFloat(Key.PANAKO_SAMPLE_RATE);
		return t * (timeResolution/sampleRate) + latency/(float) sampleRate;
	}

	private float binToHz(int f, QueryConfig config) {
		float minFreq = config.getFloat(Key.PANAKO_TRANSF_MIN_FREQ);
		float binsPerOctave = config.getFloat(Key.PANAKO_TRANSF_BANDS_PER_OCTAVE);
		float centsPerBin = 1200.0f / binsPerOctave;
		
		float diffFromMinFreqInCents = f * centsPerBin;
		float minFreqInAbsCents = (float) PitchConverter.hertzToAbsoluteCent(minFreq);
		float binInAbsCents = minFreqInAbsCents + diffFromMinFreqInCents;
		
		return (float) PitchConverter.absoluteCentToHertz(binInAbsCents);
	}

	private int mostCommonDeltaTforHitList(List<PanakoMatch> hitList) {
		Map<Integer,Integer> countPerDiff = new HashMap<>();
		hitList.forEach((hit)->{
			int deltaT = hit.deltaT();
			Integer count = countPerDiff.get(deltaT);
			countPerDiff.put(deltaT, count == null ? 1 : count + 1);
		});
		
		int maxCount = 0;
		int mostCommonDeltaT = 0;
		for(Map.Entry<Integer,Integer> entry : countPerDiff.entrySet()) {
			int count = entry.getValue();
			if(count > maxCount) {
				maxCount = count;
				mostCommonDeltaT = entry.getKey();
			}
		}
		return mostCommonDeltaT;
	}
	private static final class Alignment {
		final List<PanakoMatch> hits;
		final float queryStart, queryStop, refStart, refStop, timeFactor, frequencyFactor, percentOfSecondsWithMatches;
		Alignment(List<PanakoMatch> hits, float queryStart, float queryStop, float refStart, float refStop,
				float timeFactor, float frequencyFactor, float percentOfSecondsWithMatches) {
			this.hits = hits; this.queryStart = queryStart; this.queryStop = queryStop;
			this.refStart = refStart; this.refStop = refStop; this.timeFactor = timeFactor;
			this.frequencyFactor = frequencyFactor; this.percentOfSecondsWithMatches = percentOfSecondsWithMatches;
		}
	}

	/** Applies the tempo, pitch, filtered-hit, duration and coverage gates to one timing fit; null when any fails. */
	private Alignment alignment(List<PanakoMatch> hitlist, float slope, float offset, RobustAlignment.Fit consensus,
			float timeFactor, float frequencyFactor, double threshold, QueryConfig config) {
		//only continue processing when time factor is reasonable
		if(!(timeFactor > config.getFloat(Key.PANAKO_MIN_TIME_FACTOR) && timeFactor < config.getFloat(Key.PANAKO_MAX_TIME_FACTOR) &&
				frequencyFactor> config.getFloat(Key.PANAKO_MIN_FREQ_FACTOR) &&  frequencyFactor < config.getFloat(Key.PANAKO_MAX_FREQ_FACTOR)))
			return null;
		List<PanakoMatch> filteredHits = new ArrayList<>();
		for(PanakoMatch hit : hitlist) {
			float yActual = hit.deltaT();
			float x = hit.queryTime;
			float yPredicted = slope * x + offset;
			//should be within an expected range
			if(Math.abs(yActual-yPredicted) <= threshold
					&& (consensus == null || Math.abs((hit.matchF1-hit.queryF1)-consensus.frequencyDelta) <= 2))
				filteredHits.add(hit);
		}
		//ignore resources with too few filtered hits remaining
		if(filteredHits.size() <= config.getInt(Key.PANAKO_MIN_HITS_FILTERED))
			return null;
		float queryStart = blocksToSeconds(filteredHits.get(0).queryTime, config);
		float queryStop = blocksToSeconds(filteredHits.get(filteredHits.size()-1).queryTime, config);
		if(queryStop - queryStart < config.getFloat(Key.PANAKO_MIN_MATCH_DURATION))
			return null;
		float refStart = blocksToSeconds(filteredHits.get(0).matchTime, config);
		float refStop =  blocksToSeconds(filteredHits.get(filteredHits.size()-1).matchTime, config);
		//Construct a histogram with the number of matches for each second
		//Ideally there is a more or less equal number of matches each second
		// note that the last second might not be a full second
		Set<Integer> matchesPerSecondHistogram = new HashSet<>();
		for(PanakoMatch hit : filteredHits) {
			float offsetInSec = blocksToSeconds(hit.matchTime, config) - refStart;
			int secondBin = (int) offsetInSec;
			matchesPerSecondHistogram.add(secondBin);
		}
		//number of seconds bins
		float numberOfMatchingSeconds = (float) Math.ceil(refStop - refStart);
		float emptySeconds = numberOfMatchingSeconds - matchesPerSecondHistogram.size();
		float percentOfSecondsWithMatches = 1 - (emptySeconds / numberOfMatchingSeconds);
		if(!(percentOfSecondsWithMatches >= config.getFloat(Key.PANAKO_MIN_SEC_WITH_MATCH)))
			return null;
		return new Alignment(filteredHits, queryStart, queryStop, refStart, refStop,
				timeFactor, frequencyFactor, percentOfSecondsWithMatches);
	}
	@Override
	public void query(String query, int maxNumberOfResults, Set<Integer> avoid, QueryResultHandler handler) {
		query(query,maxNumberOfResults,avoid,handler,0,MAX_TIME);
	}


	private void query(String query, int maxNumberOfResults, Set<Integer> avoid, QueryResultHandler handler, double startTimeOffset,double numberOfSeconds ) {
		
		final QueryConfig config = new QueryConfig();
		final String queryPath ;
		List<PanakoFingerprint> prints;
		if(numberOfSeconds != MAX_TIME) {
			queryPath = query + "-" + startTimeOffset + "_" + (startTimeOffset+numberOfSeconds);
			prints = toFingerprints(query,startTimeOffset,numberOfSeconds);
		}else {
			queryPath = query;
			prints = toFingerprints(query);
		}
		
		PanakoStorage db = getStorage();
		
		Map<Long,PanakoFingerprint> printMap = new HashMap<>();
        List<Long> queryHashes = new ArrayList<>();
		
		//query
		for(PanakoFingerprint print : prints) {
			long hash = print.hash();
			db.addToQueryQueue(hash);
            queryHashes.add(hash);
			printMap.put(hash, print);
		}
		
		//fingerprint hash to info
		Map<Long,List<PanakoHit>> matchAccumulator = new HashMap<>();
		
		StopWatch w = new StopWatch();
		int queryRange = config.getInt(Key.PANAKO_QUERY_RANGE);
		db.processQueryQueue(matchAccumulator,queryRange , avoid);
		

        Map<Integer,List<PanakoMatch>> hitsPerIdentifer = groupHits(matchAccumulator, printMap);
        final List<QueryResult> queryResults = new ArrayList<>();
        Map<Long,List<PanakoHit>> verified = db.refineQuery(queryHashes, matchAccumulator, queryRange, avoid);

		 LOG.info(String.format("Query for %d prints, %d matches in %s \n", printMap.size(),matchAccumulator.size(), w.formattedToString()));

		 int minimumUnfilteredHits = config.getInt(Key.PANAKO_MIN_HITS_UNFILTERED);
		 
		 List<Integer> matchesToDelete = new ArrayList<>();
		 hitsPerIdentifer.forEach((identifier, hitlist) -> {
			 if(hitlist.size() < minimumUnfilteredHits) {
				 matchesToDelete.add(identifier);
			 }
		 });
		 
		 matchesToDelete.forEach( identifier ->{
			 hitsPerIdentifer.remove(identifier);
		 });
		 
        List<Map.Entry<Integer,List<PanakoMatch>>> candidates = new ArrayList<>(hitsPerIdentifer.entrySet());
        // Score the complete candidate separately. Extra evidence must never
        // discard a strong answer already established by the broad search.
        Map<Integer,List<PanakoMatch>> verifiedHits = groupHits(verified, printMap);
        for (Map.Entry<Integer,List<PanakoMatch>> candidate : verifiedHits.entrySet())
            if (candidate.getValue().size() >= minimumUnfilteredHits) candidates.add(candidate);
        candidates.forEach(candidateEntry -> {
            int identifier = candidateEntry.getKey();
            List<PanakoMatch> hitlist = candidateEntry.getValue();
             //sort by query time
			 Collections.sort(hitlist, (Comparator<? super PanakoMatch>) (PanakoMatch a, PanakoMatch b) -> Integer.compare(a.queryTime, b.queryTime));
			
			 //view the first and last hits (max 250)
			 int maxPartListSize = config.getInt(Key.PANAKO_HIT_PART_MAX_SIZE);
			 int partDivider = config.getInt(Key.PANAKO_HIT_PART_DIVIDER);
			 int partListLength = Math.min(maxPartListSize,Math.max(minimumUnfilteredHits,hitlist.size()/partDivider));
			 List<PanakoMatch> firstHits = hitlist.subList(0, partListLength);
			 List<PanakoMatch> lastHits  = hitlist.subList(hitlist.size() - partListLength, hitlist.size());
			 
			//find the first x1 where delta t is equals to the median delta t
			 float y1 = mostCommonDeltaTforHitList(firstHits);
			 float x1 = 0;
			 float frequencyFactor = 0;
			 for(int i = 0 ; i < firstHits.size() ; i++) {
				 PanakoMatch hit = firstHits.get(i);
				 int diff = hit.deltaT();
				 if(diff == y1) {
					 x1 = hit.queryTime;
					 frequencyFactor = binToHz(hit.matchF1, config) / binToHz(hit.queryF1, config);
					 break;
				 }
			 }

			//find the first x2 where delta t is equals to the median delta t
			 float y2 = mostCommonDeltaTforHitList(lastHits);
			 float x2 = 0;
			 for(int i = lastHits.size() - 1 ; i >= 0 ; i--) {
				 PanakoMatch hit = lastHits.get(i);
				 int diff = hit.deltaT();
				 if(diff == y2) {
					 x2 = hit.queryTime;
					 break;
				 }
			 }
			 	 
			 float slope = (y2-y1)/(x2-x1);
			 float offset = -x1 * slope + y1;
			 float timeFactor = 1.0f/(1-slope);
			 	 
			 //System.out.printf("slope %f offset %f  timefactor %f  freqfactor %f \n",slope,offset,timeFactor,frequencyFactor);
			 
			 //threshold in time bins
			 double threshold = config.getFloat(Key.PANAKO_QUERY_RANGE);

			 Alignment chosen = alignment(hitlist, slope, offset, null, timeFactor, frequencyFactor, threshold, config);
			 int robustMinimum = config.getInt(Key.PANAKO_ROBUST_ALIGNMENT_MIN_HITS);
			 if (robustMinimum >= 6 && hitlist.size() >= robustMinimum) {
			     int legacyCount = 0;
			     if (timeFactor > config.getFloat(Key.PANAKO_MIN_TIME_FACTOR)
			             && timeFactor < config.getFloat(Key.PANAKO_MAX_TIME_FACTOR)
			             && frequencyFactor > config.getFloat(Key.PANAKO_MIN_FREQ_FACTOR)
			             && frequencyFactor < config.getFloat(Key.PANAKO_MAX_FREQ_FACTOR)) {
			         for (PanakoMatch hit : hitlist)
			             if (Math.abs(hit.deltaT() - (slope * hit.queryTime + offset)) <= threshold) legacyCount++;
			     }
			     // Preserve already strong answers; spend the bounded fit only
			     // on candidates the old timing estimate could not establish.
			     if (legacyCount < robustMinimum) {
			         int[] q = new int[hitlist.size()], r = new int[q.length], f = new int[q.length];
			         for (int i = 0; i < q.length; i++) {
			             PanakoMatch hit = hitlist.get(i);
			             q[i] = hit.queryTime; r[i] = hit.matchTime; f[i] = hit.matchF1 - hit.queryF1;
			         }
			         float bands = config.getFloat(Key.PANAKO_TRANSF_BANDS_PER_OCTAVE);
			         // Every recovery fit passes the same result gates as the legacy
			         // fit before it can win, so a short burst cannot hide a valid
			         // fit and the legacy result stands when no recovery fit passes.
			         List<RobustAlignment.Fit> recoveryFits = verifiedHits.get(identifier) == hitlist
                         ? RobustAlignment.verifiedFits(q, r, f, robustMinimum, threshold,
                                 config.getFloat(Key.PANAKO_MIN_TIME_FACTOR), config.getFloat(Key.PANAKO_MAX_TIME_FACTOR),
                                 config.getFloat(Key.PANAKO_MIN_FREQ_FACTOR), config.getFloat(Key.PANAKO_MAX_FREQ_FACTOR), bands)
                         : RobustAlignment.fits(q, r, f, robustMinimum, threshold,
			                 config.getFloat(Key.PANAKO_MIN_TIME_FACTOR), config.getFloat(Key.PANAKO_MAX_TIME_FACTOR),
			                 config.getFloat(Key.PANAKO_MIN_FREQ_FACTOR), config.getFloat(Key.PANAKO_MAX_FREQ_FACTOR), bands);
                 for (RobustAlignment.Fit fit : recoveryFits) {
			             Alignment candidate = alignment(hitlist, fit.slope, fit.offset, fit, 1.0f / (1 - fit.slope),
			                     (float) Math.pow(2, fit.frequencyDelta / bands), threshold, config);
			             // Fits arrive ranked by aligned hits, which the gate's filtered
			             // count reproduces, so the first passing fit is the best one.
			             if (candidate != null) {
			                 if (chosen == null || candidate.hits.size() > chosen.hits.size()) chosen = candidate;
			                 break;
			             }
			         }
			     }
			 }
			 if (chosen != null) {
				 //retrieve meta-data
				 PanakoResourceMetadata metadata = db.getMetadata((long) identifier);
				 String refPath = "metadata unavailable!";
				 if(metadata != null )
					 refPath = metadata.path;
				 QueryResult r = new QueryResult(queryPath, chosen.queryStart, chosen.queryStop, refPath, "" + identifier,
						 chosen.refStart, chosen.refStop, chosen.hits.size(), chosen.timeFactor, chosen.frequencyFactor, chosen.percentOfSecondsWithMatches);
				 queryResults.add(r);
			 }
		 });
		 
		 if (queryResults.isEmpty()) {
			 handler.handleEmptyResult(QueryResult.emptyQueryResult(queryPath,0,0));
		 }else {
			 //sort results by score desc
			 queryResults.sort((Comparator<? super QueryResult>) (QueryResult a, QueryResult b) -> Integer.compare((int) b.score, (int) a.score));
            Set<String> answered = new HashSet<>();
            queryResults.removeIf(result -> !answered.add(result.refIdentifier));
			 //limit results to max number of results, if needed
			 List<QueryResult> finalResults = queryResults;
			 if(queryResults.size()>maxNumberOfResults) {
				 finalResults = queryResults.subList(0, maxNumberOfResults);
			 }
			 //handle the results in order (best score first)
			 for(QueryResult r : finalResults) {
				 handler.handleQueryResult(r);
			 }
		 }
	}
	

    private Map<Integer,List<PanakoMatch>> groupHits(Map<Long,List<PanakoHit>> accumulator,
            Map<Long,PanakoFingerprint> printMap) {
        Map<Integer,List<PanakoMatch>> grouped = new HashMap<>();
		 accumulator.forEach((fingerprintHash, dbHits) -> {

			 PanakoFingerprint print = printMap.get(fingerprintHash);
			 dbHits.forEach((dbHit)->{
				//long matchingHash  = data[0];
				 int identifier = dbHit.resourceID;
				 int matchTime = dbHit.t;
				 List<PanakoMatch> group = grouped.get(identifier);
				 if(group == null){
					group = new ArrayList<>();
					grouped.put(identifier, group);
				 }
				 PanakoMatch hit = new PanakoMatch();
				 hit.identifier = identifier;
				 hit.matchTime = matchTime;
				 hit.originalHash = dbHit.originalHash;
				 hit.matchedNearHash = dbHit.matchedNearHash;
				 hit.queryTime = print.t1;

				 hit.queryF1 = print.f1;
				 hit.matchF1 = dbHit.f;

				 group.add(hit);
			 });
		 });

        return grouped;
    }

	@Override
	public void monitor(String query, int maxNumberOfReqults, Set<Integer> avoid, QueryResultHandler handler) {
		int overlapInSeconds = Config.getInt(Key.MONITOR_OVERLAP); // 5
		int stepSizeInSeconds = Config.getInt(Key.MONITOR_STEP_SIZE); //25
		
		float totalDuration = AudioFileUtils.audioFileDurationInSeconds(new File(query));
		
		//Steps: 0-25s ; 20-45s ; 40-65s ...
		int actualStep = stepSizeInSeconds - overlapInSeconds;//20s
		for(int t = 0 ; t + stepSizeInSeconds < totalDuration; t += actualStep ) {			
			query(query,maxNumberOfReqults,avoid,handler,t,stepSizeInSeconds);
		}
	}

	@Override
	public boolean hasResource(String resource) {
		int identifier = FileUtils.getIdentifier(resource);
		PanakoStorage db = getStorage();

		return db.getMetadata(identifier) != null;
	}

	@Override
	public boolean isStorageAvailable() {
		return true;
	}

	@Override
	public void printStorageStatistics() {
		getStorage().printStatistics(true);
	}

	@Override
	public String resolve(String filename) {
		return "" + FileUtils.getIdentifier(filename);
	}
	
	private List<long[]> readFingerprintFile(String fingerprintFilePath) {
		List<long[]> prints = new ArrayList<>();
		try {
			PanakoStorageFile fileDb = PanakoStorageFile.getInstance();
			final File file = new File(fingerprintFilePath);
			FileReader fileReader = new FileReader(file);
			final BufferedReader reader = new BufferedReader(fileReader);
			String inputLine = reader.readLine();
			while (inputLine != null) {
				long[] data = fileDb.dataFromLine(inputLine);
				prints.add(data);
				inputLine = reader.readLine();
			}
			reader.close();
		} catch (final IOException i1) {
			System.err.println("Can't open file:" + fingerprintFilePath);
			i1.printStackTrace();
		}
		return prints;
	}
	
	
	private void addToMap(TreeMap<Integer,float[]> map,int t,int f,float m) {
		if(!map.containsKey(t)) {
			map.put(t, new float[Config.getInt(Key.OLAF_SIZE)/2]);
		}
		map.get(t)[f]=m;
	}

	@Override
	public void print(String path, boolean sonicVisualizerOutput, boolean printOnlyEPs) {
		List<PanakoFingerprint> prints = toFingerprints(path);

		TreeMap<Integer,float[]> timeIndexedSpectralPeaks = new TreeMap<>();
		for(PanakoFingerprint print : prints) {
			addToMap(timeIndexedSpectralPeaks,print.t1,print.f1,print.m1);
			addToMap(timeIndexedSpectralPeaks,print.t2,print.f2,print.m2);
			addToMap(timeIndexedSpectralPeaks,print.t2,print.f2,print.m3);
		}
		float[] emptySpectrum = new float[Config.getInt(Key.OLAF_SIZE)/2];
		StringBuilder sb = new StringBuilder();
		for(int t = 0 ; t <= timeIndexedSpectralPeaks.lastKey();t++) {
			float[] spectrum = emptySpectrum;
			if(timeIndexedSpectralPeaks.containsKey(t))
				spectrum = timeIndexedSpectralPeaks.get(t);
			
			sb.append(blocksToSeconds(t)).append(",");
			for(int i = 0 ; i < spectrum.length ; i++) {
				sb.append(spectrum[i]).append(",");
			}
			sb.append("\n");
		}
		System.out.println(sb);
	}

	@Override
	public void clear() {
		getStorage().clear();
	}

	public String metadata(String path) {
		final PanakoStorage db = getStorage();
		long identifier = FileUtils.getIdentifier(path);
		PanakoResourceMetadata metaData = db.getMetadata(identifier);

		return String.format("%d ; %s ; %.3f (s) ; %d (#) ; %.3f (#/s)",metaData.identifier,metaData.path,metaData.duration,metaData.numFingerprints,metaData.printsPerSecond());
	}
}
