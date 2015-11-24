package lrstorm;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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
				if (collector.isEmpty())
				for (AccidentTuple t : collector) {
					Values v = t.toValues();
					v.add(false);
					results.add(t.toValues());
				}

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

		// TODO isAccident is nto an appropriate name for the field
		builder.setBolt(
				"checkAccidents",
				new ViperBolt(new Fields("segment", "isAccident", "accidentTS",
						"isHeartBeat"), new DetectAccidentsFunction()))
				.shuffleGrouping("filter");

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

		class Merger implements BoltFunction {

			Queue<Tuple> accidentsQueue;
			boolean receivedAccidentsFlush;
			Queue<Tuple> vehiclesQueue;
			boolean receivedVehiclesFlush;

			private void bufferIncomingTuple(Tuple t) {
				if (t.getSourceComponent().equals("checkNewSegment")) {
					vehiclesQueue.add(t);
				} else if (t.getSourceComponent().equals("checkAccidents")) {
					accidentsQueue.add(t);
				} else
					throw new RuntimeException(
							"Unknown source component for tuple " + t);
			}

			private List<Tuple> getReadyTuples() {
				List<Tuple> result = new ArrayList<Tuple>();
				while (!accidentsQueue.isEmpty() && !vehiclesQueue.isEmpty()) {

					long accidentTS = accidentsQueue.peek().getLongByField(
							"accidentTS");
					LRTuple posrep = (LRTuple) vehiclesQueue.peek()
							.getValueByField("posrep");

					if (accidentTS <= posrep.time) {
						result.add(accidentsQueue.poll());
					} else {
						result.add(vehiclesQueue.poll());
					}
				}
				return result;
			}

			public List<Values> process(Tuple arg0) {

				List<Values> results = new ArrayList<Values>();

				bufferIncomingTuple(arg0);
				List<Tuple> readyTuples = getReadyTuples();
				for (Tuple t : readyTuples) {
					System.out.println(t.getSourceComponent() + "/"
							+ t.getSourceStreamId() + "/" + t.getSourceTask()
							+ ": " + t.toString());
				}
				return results;
			}

			public void prepare(Map arg0, TopologyContext arg1) {
				accidentsQueue = new LinkedList<Tuple>();
				vehiclesQueue = new LinkedList<Tuple>();
				receivedAccidentsFlush = false;
				receivedVehiclesFlush = false;
			}

			public List<Values> receivedFlush(Tuple t) {
				if (t.getSourceComponent().equals("checkNewSegment")) {
					receivedVehiclesFlush = true;
				} else if (t.getSourceComponent().equals("checkAccidents")) {
					receivedAccidentsFlush = true;
				}

				if (receivedVehiclesFlush && receivedAccidentsFlush) {
					List<Values> results = new ArrayList<Values>();

					List<Tuple> readyTuples = getReadyTuples();
					for (Tuple readyT : readyTuples) {
						System.out.println(readyT.getSourceComponent() + "/"
								+ readyT.getSourceStreamId() + "/"
								+ readyT.getSourceTask() + ": "
								+ readyT.toString());
					}

					System.out.println("this.accidentsQueue.size(): "
							+ this.accidentsQueue.size());
					System.out.println("this.vehiclesQueue.size(): "
							+ this.vehiclesQueue.size());

					assert ((this.vehiclesQueue.isEmpty() && !this.accidentsQueue
							.isEmpty()) || (!this.vehiclesQueue.isEmpty() && this.accidentsQueue
							.isEmpty()));
					if (this.vehiclesQueue.isEmpty())
						while (!this.accidentsQueue.isEmpty()) {
							Tuple out = this.accidentsQueue.poll();
							System.out.println(out.getSourceComponent() + "/"
									+ t.getSourceStreamId() + "/"
									+ out.getSourceTask() + ": "
									+ out.toString());
						}
					if (this.accidentsQueue.isEmpty())
						while (!this.vehiclesQueue.isEmpty()) {
							Tuple out = this.vehiclesQueue.poll();
							System.out.println(out.getSourceComponent() + "/"
									+ t.getSourceStreamId() + "/"
									+ out.getSourceTask() + ": "
									+ out.toString());
						}

					return results;
				}
				return null;
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
