package docsim;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.*;
import org.apache.hadoop.util.*;

public class buildVocab extends Configured implements Tool {
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {
		 static enum Counters { INPUT_WORDS };
		 private final static IntWritable one = new IntWritable(1);
         private Text word = new Text();

         DistributedCache ds = new DistributedCache();
         
         private boolean caseSensitive = false;
         private Set<String> patternsToSkip = new HashSet<String>();
         private HashMap<Integer,String> docIdMap = new HashMap<Integer,String>(); 
         private long numRecords = 0;
         private String inputFile,docidFile;

         public void configure(JobConf job) {
           caseSensitive = job.getBoolean("docsim.case.sensitive", true);
           inputFile = job.get("map.input.file");
          
           if (job.getBoolean("docsim.skip.patterns", true)) {
             Path[] patternsFiles = new Path[0];
             try {
               patternsFiles = DistributedCache.getLocalCacheFiles(job);
             } catch (IOException ioe) {
               System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
             }
             for (Path patternsFile : patternsFiles) {
            	 if(patternsFile.getName().matches("docid_map.txt"))
                	 parseSkipFile(patternsFile);
             }
           }
           
           if (job.getBoolean("docsim.docids.map", true)) {
               Path[] docidMapFile = new Path[1];
               try {
                 docidMapFile = DistributedCache.getLocalCacheFiles(job);
               } catch (IOException ioe) {
                 System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
               }
               for (Path docidFile : docidMapFile) {
                 if(docidFile.getName().matches("docid_map.txt"))
                	 loadDocIdMap(docidFile);
               }
             }
             
         }

         private void parseSkipFile(Path patternsFile) {
           try {
             BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
             String pattern = null;
             while ((pattern = fis.readLine()) != null) {
               patternsToSkip.add(pattern);
             }
           } catch (IOException ioe) {
             System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
           }
         }

         private void loadDocIdMap(Path docIdsFile) {
             try {
               BufferedReader fis = new BufferedReader(new FileReader(docIdsFile.toString()));
               String pattern = null;
               Integer key;
               String value;
               while ((pattern = fis.readLine()) != null) {
            	    StringTokenizer st1 = new StringTokenizer(pattern);
            	    key = Integer.parseInt(st1.nextToken());
            	    value = st1.nextToken();
            	    docIdMap.put(key, value);
               }
             } catch (IOException ioe) {
               System.err.println("Caught exception while parsing the cached file '" + docIdMap + "' : " + StringUtils.stringifyException(ioe));
             }
           }

         public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
           String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();

           for (String pattern : patternsToSkip) {
             line = line.replaceAll(pattern, "");
           }

           StringTokenizer tokenizer = new StringTokenizer(line);
           while (tokenizer.hasMoreTokens()) {
             word.set(tokenizer.nextToken());
             output.collect(word, one);
             reporter.incrCounter(Counters.INPUT_WORDS, 1);
           }

           if ((++numRecords % 100) == 0) {
             reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
           }
         }
       }

       public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
         public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
           int sum = 0;
           while (values.hasNext()) {
             sum += values.next().get();
           }
           output.collect(key, new IntWritable(sum));
         }
       }

       public int run(String[] args) throws Exception {
         JobConf conf = new JobConf(getConf(), buildVocab.class);
         conf.setJobName("vocab");

         conf.setOutputKeyClass(Text.class);
         conf.setOutputValueClass(IntWritable.class);

         conf.setMapperClass(Map.class);
         conf.setCombinerClass(Reduce.class);
         conf.setReducerClass(Reduce.class);
         conf.set(XmlInputFormat.START_TAG_KEY, "<body>");
         conf.set(XmlInputFormat.END_TAG_KEY, "</body>");
         conf.setInputFormat(XmlInputFormat.class);
         conf.setOutputFormat(TextOutputFormat.class);
         
         conf.setNumMapTasks(20);
         conf.setNumReduceTasks(20);
         

         List<String> other_args = new ArrayList<String>();
         for (int i=0; i < args.length; ++i) {
           if ("-skip".equals(args[i])) {
             DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
             conf.setBoolean("docsim.skip.patterns", true);
           } 
           else if ("-docmap".equals(args[i])) {
               DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
               conf.setBoolean("docsim.docids.map", true);
             } 
           else {
             other_args.add(args[i]);
           }
         }

         FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
         FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

         JobClient.runJob(conf);
         return 0;
       }

       public static void main(String[] args) throws Exception {
         String inpath = args[0];
         String outpath = "vocab.txt";
         String paths[]= { inpath, outpath };
    
        int res = ToolRunner.run(new Configuration(), new buildVocab(), paths);
         System.exit(res);
       }   
}

