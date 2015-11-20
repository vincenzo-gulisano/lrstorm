package lrstorm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import lrstorm.AccidentSegmentState.UpdateSegmentAnswer;

//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;

public class DetectAccidentOperator implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1447389627327330715L;
	private HashMap<String, AccidentSegmentState> segments;

	public DetectAccidentOperator() {
		segments = new HashMap<String, AccidentSegmentState>();
	}

	public void run(LRTuple t, ArrayList<AccidentTuple> collector) {
		run(t.xway, t.seg, t.time, t.vid, t.speed, t.lane, t.pos, collector);
	}

	public void run(int xway, int segment, long time, int vid, int speed,
			int lane, int position, ArrayList<AccidentTuple> collector) {
		// check if vehicle is stopped or not
		String segid = xway + "_" + segment;
		AccidentSegmentState segstate = segments.get(segid);
		if (segstate == null) {
			segstate = new AccidentSegmentState();
			segments.put(segid, segstate);
		}

		// check for accidents
		UpdateSegmentAnswer answer = segstate.updateSegmentState(vid, time,
				xway, lane, segment, position, speed);
		// we output xway_segment, is_accident for all the 4 upstream
		// segments (if is accident)
		// is connected with position stream
		// in the next operator we update an internal state holding the
		// segments_with_accidents
		// and we output accident notification for vehicle
		if (answer.newacc) {
			for (int i = 0; i <= 4; i++) {
				int realseg = segment - i;
				if (realseg < 0)
					break;
				collector.add(new AccidentTuple(xway + "_" + realseg, true,
						time));
			}
		} else {
			if (answer.cleared) {
				collector.add(new AccidentTuple(xway + "_" + segment, false,
						time));
				for (int i = 1; i <= 4; i++) {
					int realseg = segment - i;
					if (realseg < 0)
						break;
					collector.add(new AccidentTuple(xway + "_" + realseg,
							false, time));
				}
			}
		}
	}

}