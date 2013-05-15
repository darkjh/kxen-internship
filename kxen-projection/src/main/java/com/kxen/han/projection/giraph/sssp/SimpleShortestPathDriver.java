package com.kxen.han.projection.giraph.sssp;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimpleShortestPathDriver implements Tool {
	
	static {
		Configuration.addDefaultResource("giraph-site.xml");
	}
	
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
	    if (null == getConf()) {
	        conf = new Configuration();
	    }
	    
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());
		giraphConf.setVertexClass(SimpleShortestPathVertex.class);
		giraphConf.setVertexInputFormatClass(JsonLongDoubleFloatDoubleVertexInputFormat.class);
		giraphConf.setVertexOutputFormatClass(JsonLongDoubleFloatDoubleVertexOutputFormat.class);
		giraphConf.setWorkerConfiguration(4, 4, 100.0f);
		giraphConf.setLong(SimpleShortestPathVertex.SOURCE_ID, Long.parseLong(args[2]));
		// giraphConf.setVertexValueFactoryClass(vertexValueFactoryClass)
		GiraphJob job = new GiraphJob(giraphConf, "SimpleShortestPath");
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(args[0]));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(args[1]));
		
		if (job.run(true))
			return 0;
		else 
			return -1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SimpleShortestPathDriver(), args);
	}
}
