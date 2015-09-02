package org.apache.accumulo.examples.simple.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	private static Options opts;
	private static Option passwordOpt;
	private static Option usernameOpt;
	private static String USAGE = "wordCount <instance name> <zoo keepers> <input dir> <output table>";

	static {
		usernameOpt = new Option("u", "username", true, "username");
		passwordOpt = new Option("p", "password", true, "password");

		opts = new Options();

		opts.addOption(usernameOpt);
		opts.addOption(passwordOpt);
	}

	public static class MapClass extends Mapper<LongWritable, Text, Text, Mutation> {
		// @Override
		public void map(LongWritable key, Text value, Context output) throws IOException {

		HashMap<String, String> tag =new HashMap<String, String>();
		tag.put("Celtics","Boston Celtics");
		tag.put("Knicks","New York Knicks");
		tag.put("76ers","Philadelphia 76ers");
		tag.put("Nets","New Jersey Nets");
		tag.put("Raptors","Toronto Raptors");
		tag.put("Bulls","Chicago Bulls");
		tag.put("Pacers","Indiana Pacers");
		tag.put("Bucks","Milwaukee Bucks");
		tag.put("Pistons","Detroit Pistons");
		tag.put("Cavs","Cleveland Cavaliers");
		tag.put("MiamiHeat","Miami Heat");
		tag.put("OrlandoMagic","Orlando Magic");
		tag.put("Hawks","Atlanta Hawks");
		tag.put("Bobcats","Charlotte Bobcats");
		tag.put("Wizards","Washington Wizards");
		tag.put("okcthunder","Oklahoma City");
		tag.put("Nuggets","Denver Nuggets");
		tag.put("TrailBlazers","Portland Trailblazers");
		tag.put("UtahJazz","Utah Jazz");
		tag.put("TWolves","Minnesota Timberwolves");
		tag.put("Lakers","L A Lakers");
		tag.put("Suns","Phoenix Suns");
		tag.put("GSWarriors","Golden State Warriors");
		tag.put("Clippers","L.A.Clippers");
		tag.put("NBAKings","Sacramento Kings");
		tag.put("GoSpursGo","San Antonio Spurs");
		tag.put("Mavs","Dallas Mavericks");
		tag.put("Hornets","New Orleans Hornets");
		tag.put("Grizzlies","Memphis Grizzlies");
		tag.put("Rockets","Houston Rockets");
		
	    List<String> east = new ArrayList<String>(Arrays.asList("Celtics", "Knicks","76ers","Nets","Raptors","Bulls","Pacers","Bucks","Pistons","Cavs","MiamiHeat","OrlandoMagic","Hawks","Bobcats","Wizards"));
	    List<String> west = new ArrayList<String>(Arrays.asList("okcthunder","Nuggets","TrailBlazers","UtahJazz","TWolves","Lakers","Suns","GSWarriors","Clippers","NBAKings","GoSpursGo","Mavs","Hornets","Grizzlies","Rockets"));

    	FileSplit fSplit = (FileSplit)output.getInputSplit();
    	String fname = fSplit.getPath().getName();
    	fname = fname.substring(0, fname.length()-4);
    	
    	if(fname.charAt(fname.length()-1)=='.')
    		fname = fname.substring(0,fname.length()-1);
      
    	Mutation mutation = null;
    	
    	if(east.contains(fname))
    	{
	    	Mutation mutation1 = new Mutation(new Text(tag.get(fname)));
	    	mutation1.put(new Text("#"+fname),new Text(""), new ColumnVisibility("east"), new Value("0".getBytes()));
	    	try {
				output.write(new Text("win"), mutation1);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
	    	
	    	Mutation mutation2 = new Mutation(new Text(tag.get(fname)));
	    	mutation2.put(new Text("#"+fname),new Text(""), new ColumnVisibility("west"), new Value("0".getBytes()));
	    	try {
				output.write(new Text("lose"), mutation2);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
    	}
    	if(west.contains(fname))
    	{
    		Mutation mutation1 = new Mutation(new Text(tag.get(fname)));
        	mutation1.put( new Text("#"+fname),new Text(""), new ColumnVisibility("west"), new Value("0".getBytes()));
        	try {
    			output.write(new Text("win"), mutation1);
    		} catch (InterruptedException e1) {
    			e1.printStackTrace();
    		}
        	
    		Mutation mutation2 = new Mutation(new Text(tag.get(fname)));
        	mutation2.put( new Text("#"+fname),new Text(""), new ColumnVisibility("west"), new Value("0".getBytes()));
        	try {
    			output.write(new Text("lose"), mutation2);
    		} catch (InterruptedException e1) {
    			e1.printStackTrace();
    		}	
    	}
    	
    	
    	String[] words = value.toString().split("\\s+");

    	for (String word : words) {
                       
        if(!word.equals("win") && !word.equals("lose") && !word.equals("Win") && !word.equals("Lose"))
        {
        	continue;        	
        }
        else{
        	mutation = new Mutation(new Text(tag.get(fname)));
        	if(east.contains(fname))
        	{
        		mutation.put(new Text("#"+fname),new Text(""), new ColumnVisibility("east"), new Value("1".getBytes()));	
        	}
        	else if(west.contains(fname))
        	{
        		mutation.put(new Text("#"+fname), new Text(""), new ColumnVisibility("west"), new Value("1".getBytes()));
        	}
        }
                        
        try {
        	if(word.equals("win") || word.equals("Win"))
        	{
        		output.write(new Text("win"), mutation);
        	}
        	if(word.equals("lose") || word.equals("Lose"))
        	{
        		output.write(new Text("lose"), mutation);
        	}
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
	}

	public int run(String[] unprocessed_args) throws Exception {
		Parser p = new BasicParser();

		CommandLine cl = p.parse(opts, unprocessed_args);
		String[] args = cl.getArgs();

		String username = cl.getOptionValue(usernameOpt.getOpt(), "root");
		String password = cl.getOptionValue(passwordOpt.getOpt(), "secret");

		if (args.length != 4) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ args.length + " instead of 4.");
			return printUsage();
		}

		Job job = new Job(getConf(), WordCount.class.getName());
		job.setJarByClass(this.getClass());

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, new Path(args[2]));

		job.setMapperClass(MapClass.class);

		job.setNumReduceTasks(0);

		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), username,password.getBytes(), true, args[3]);
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(),args[0], args[1]);
		job.waitForCompletion(true);
		return 0;
	}

	private int printUsage() {
		HelpFormatter hf = new HelpFormatter();
		hf.printHelp(USAGE, opts);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(CachedConfiguration.getInstance(),
				new WordCount(), args);
		System.exit(res);
	}
}