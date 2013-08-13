package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.BitSet;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.filters.DefaultEdgeInputFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VLongWritable;


/**
 * An input filter class
 * Every input thread will have a copy of the frequent item map, which is generated
 * previously by counting and stored in the hadoop ditributed cache.
 * When Giraph framework reads a graph edge, the dropEdge method get called and its
 * return value decides whether this edge will be dropped
 * 
 * @author Han JU
 *
 */
public class EdgeBySupportFilter 
extends DefaultEdgeInputFilter<VLongWritable,NullWritable> {
	
	private static BitSet FREQ_PROJ;
	private static BitSet FREQ_EMIT;
	
	private int dCacheFileIndex = 0;
	private static boolean init = false;
	private static boolean megaHub;
	
	/** 
	 * Initialize the frequent item bitmap from distributed cache
	 * do it only once per thread
	 * If mega-hub detection is on, initialize an additional bitmap which 
	 * contains the emit-side nodes that are to keep (non mega-hub nodes)
	 */
	private void initFreq() throws IOException {
		GiraphConfiguration conf = getConf();
		megaHub = conf.getBoolean(GiraphProjection.MEGA_HUB, false);
		FREQ_PROJ = readCachedFList(conf);
		if (megaHub) {
			FREQ_EMIT = readCachedFList(conf);
		}
	}
	
	private BitSet readCachedFList(Configuration conf)
			throws IOException {
		System.out.println("Init freq list ...");
		Path[] caches = DistributedCache.getLocalCacheFiles(conf);
		FileSystem fs = FileSystem.getLocal(conf); // cache is stored locally
		// dCache file is indexed, so increment it once we accessed a file
		Path fListPath = fs.makeQualified(caches[dCacheFileIndex++]);
		ObjectInputStream ois = new ObjectInputStream(fs.open(fListPath));
		BitSet bs = null;
		try {
			bs = (BitSet) ois.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			ois.close();
		}
		return bs;
	}
	
	/** 
	 * If an edge's target vertex (product node) is not frequent
	 * (isn't included in the map), drop it 
	 * If mega-hub detection is on, also drop mega-hub nodes by consulting
	 * the two bitmaps
	 */
	@Override
	public boolean dropEdge(VLongWritable sourceId, 
			Edge<VLongWritable,NullWritable> edge) {
		if (!init) {
			init = true;
			try {
				initFreq();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		long target = edge.getTargetVertexId().get();
		// remember that for distinguishing the id space, source (emit-side)  
		// is negative, so when reading from a bitmap, need to negate it back
		boolean drop = (megaHub && !FREQ_EMIT.get(-(int)sourceId.get())) 
				|| !FREQ_PROJ.get((int)target);  
		return drop;
	}
}