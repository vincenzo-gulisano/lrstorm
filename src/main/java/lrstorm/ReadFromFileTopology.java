package lrstorm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import operator.csvSink.CSVFileWriter;
import operator.csvSink.CSVSink;
import operator.csvSpout.CSVFileReader;
import operator.csvSpout.CSVReaderSpout;
import operator.viperBolt.BoltFunction;
import operator.viperBolt.BoltFunctionBase;
import operator.viperBolt.ViperBolt;
import topology.ViperTopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ReadFromFileTopology {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {

		boolean local = true;
		boolean logStats = true;
		String statsPath = "/Users/vinmas/repositories/lrstorm/logs/";
		final int spout_parallelism = 1;
		String topologyName = "lr";

		ViperTopologyBuilder builder = new ViperTopologyBuilder();

		// CREATE INJECTOR

		builder.setSpout("spout", new CSVReaderSpout(new CSVFileReader() {

			@Override
			protected Values convertLineToTuple(String line) {
				// System.out.println("Converting line " + line);
				return new Values(new LRTuple(line));
			}

		}, new Fields("posrep")), spout_parallelism);

		// FILTER BASED ON THE TYPE OF TUPLE (FORWARD POSITION REPORTS ONLY)

		class Filter extends BoltFunctionBase {

			@Override
			public List<Values> process(Tuple arg0) {
				List<Values> results = new ArrayList<Values>();
				LRTuple lrTuple = (LRTuple) arg0.getValueByField("posrep");
				if (lrTuple.type == 0)
					results.add(new Values(lrTuple));
				return results;
			}

		}
		builder.setBolt("filter",
				new ViperBolt(new Fields("posrep"), new Filter()))
				.shuffleGrouping("spout");

		// CHECK WHICH TUPLES REFER TO A VEHICLE ENTERING A NEW SEGMENT

		class CheckNewSegment implements BoltFunction {

			private DetectNewVehicles detectNewVehicles;

			public List<Values> process(Tuple arg0) {
				List<Values> results = new ArrayList<Values>();
				LRTuple lrTuple = (LRTuple) arg0.getValueByField("posrep");
				results.add(new Values(lrTuple, detectNewVehicles
						.isThisANewVehicle(lrTuple)));
				return results;
			}

			@SuppressWarnings("rawtypes")
			public void prepare(Map arg0, TopologyContext arg1) {
				detectNewVehicles = new DetectNewVehicles();
			}

			public List<Values> receivedFlush(Tuple arg0) {
				return new ArrayList<Values>();
			}

		}
		builder.setBolt(
				"checkNewSegment",
				new ViperBolt(new Fields("posrep", "newSeg"),
						new CheckNewSegment())).shuffleGrouping("filter");

		// CHECK FOR ACCIDENTS

		class DetectAccidentsFunction implements BoltFunction {

			private DetectAccidentOperator detectAccidentsOp;

			public List<Values> process(Tuple arg0) {
				List<Values> results = new ArrayList<Values>();
				LRTuple lrTuple = (LRTuple) arg0.getValueByField("posrep");

				ArrayList<AccidentTuple> collector = new ArrayList<AccidentTuple>();
				detectAccidentsOp.run(lrTuple, collector);

				// TODO this could be optimized
				for (AccidentTuple t : collector)
					results.add(t.toValues());

				return results;
			}

			@SuppressWarnings("rawtypes")
			public void prepare(Map arg0, TopologyContext arg1) {
				detectAccidentsOp = new DetectAccidentOperator();
			}

			public List<Values> receivedFlush(Tuple arg0) {
				return new ArrayList<Values>();
			}

		}

		builder.setBolt(
				"checkAccidents",
				new ViperBolt(
						new Fields("segment", "isAccident", "accidentTS"),
						new DetectAccidentsFunction())).shuffleGrouping(
				"filter");

		// LOG checkNewSegment TO DISK (TEMPORARY)

		builder.setBolt("checkNewSegmentSink", new CSVSink(new CSVFileWriter() {

			@Override
			protected String convertTupleToLine(Tuple t) {
				LRTuple lrTuple = (LRTuple) t.getValueByField("posrep");
				boolean newSegment = t.getBooleanByField("newSeg");
				return lrTuple.toString() + "," + newSegment;
			}

		}), 1).shuffleGrouping("checkNewSegment");

		// LOG checkAccidents TO DISK (TEMPORARY)

		builder.setBolt("checkAccidentsSink", new CSVSink(new CSVFileWriter() {

			@Override
			protected String convertTupleToLine(Tuple t) {
				return t.getStringByField("segment") + ","
						+ t.getBooleanByField("isAccident") + ","
						+ t.getLongByField("accidentTS");

			}

		}), 1).shuffleGrouping("checkAccidents");

		// LOG RESULTS TO DISK

		builder.setBolt("sink", new CSVSink(new CSVFileWriter() {

			@Override
			protected String convertTupleToLine(Tuple t) {
				LRTuple lrTuple = (LRTuple) t.getValueByField("posrep");
				return lrTuple.toString();
			}

		}), 1).shuffleGrouping("filter");

		// MERGE ACCIDENTS AND VEHICLES

		class Merger extends BoltFunctionBase {

			@Override
			public List<Values> process(Tuple arg0) {
				List<Values> results = new ArrayList<Values>();
				System.out.println(arg0.getSourceComponent() + "/"
						+ arg0.getSourceStreamId() + "/" + arg0.getSourceTask()
						+ ": " + arg0.toString());
				return results;
			}

		}
		builder.setBolt("merger", new ViperBolt(new Fields(), new Merger()))
				.shuffleGrouping("checkNewSegment")
				.shuffleGrouping("checkAccidents");

		// CONFIGURE TOPOLOGY AND SUBMIT

		Config conf = new Config();
		conf.setDebug(false);

		conf.put("log.statistics", logStats);
		conf.put("log.statistics.path", statsPath);

		// TODO this is not the best way to pass these parameters...
		// Set the file name for the reader
		conf.put("spout.0.filepath",
				"/Users/vinmas/repositories/lrstorm/data/cardatapoints_half.out");
		conf.put("sink.0.filepath",
				"/Users/vinmas/repositories/lrstorm/logs/lr.out");
		conf.put("checkNewSegmentSink.0.filepath",
				"/Users/vinmas/repositories/lrstorm/logs/lr_newvehicles.out");
		conf.put("checkAccidentsSink.0.filepath",
				"/Users/vinmas/repositories/lrstorm/logs/lr_accidents.out");

		if (!local) {
			conf.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());

			Thread.sleep(600000);

			cluster.shutdown();
		}

	}
}
