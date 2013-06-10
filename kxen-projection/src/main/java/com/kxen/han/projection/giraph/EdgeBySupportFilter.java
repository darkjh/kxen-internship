package com.kxen.han.projection.giraph;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.filters.DefaultEdgeInputFilter;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;

/**
 * An input filter class
 * Every input thread will have a copy of the frequent item map, which is generated
 * previously by counting and stored in the hadoop ditributed cache.
 * When Giraph framework reads a graph edge, the dropEdge method get called and its
 * return value decides whether this edge will be kept.
 * 
 * @author Han JU
 *
 */
public class EdgeBySupportFilter 
extends DefaultEdgeInputFilter<VLongWritable,VLongWritable> {
	
	private static Map<Long,Long> FREQ;
	
	/** 
	 * Initialize the frequent item map from distributed cache
	 * do it only once per thread 
	 */
	@SuppressWarnings("unchecked")
	private void initFreq() throws IOException {
		GiraphConfiguration conf = getConf();
		if (null == FREQ) {
			System.out.println("Init freq list ...");
			Path[] caches = DistributedCache.getLocalCacheFiles(conf);
			FileSystem fs = FileSystem.getLocal(conf); // cache is stored locally
			Path fListPath = fs.makeQualified(caches[0]);
			ObjectInputStream ois = new ObjectInputStream(fs.open(fListPath));
			try {
				FREQ = (Map<Long, Long>) ois.readObject();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} finally {
				ois.close();
			}
		}
	}
	
	/** 
	 * If an edge's target vertex (product node) is not frequent
	 * (isn't included in the map), drop it 
	 */
	@Override
	public boolean dropEdge(VLongWritable sourceId, 
			Edge<VLongWritable,VLongWritable> edge) {
		if (null == FREQ)
			try {
				initFreq();
			} catch (IOException e) {
				e.printStackTrace();
			}
		long target = edge.getTargetVertexId().get();
		return !FREQ.containsKey(target);
	}
}
