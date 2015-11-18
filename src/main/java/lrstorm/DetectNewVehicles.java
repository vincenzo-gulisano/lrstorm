package lrstorm;

import java.util.ArrayList;
import java.util.HashMap;

class DetectNewVehicles {

	class SegmentInfo {
		public SegmentInfo(String segment, Long lastTimeUpdate) {
			this.segment = segment;
			this.lastTimeUpdate = lastTimeUpdate;
		}

		public String segment;
		public Long lastTimeUpdate;
	}
	
	public DetectNewVehicles() {
		previousSegments = new HashMap<Integer, DetectNewVehicles.SegmentInfo>();
	}

	private HashMap<Integer, SegmentInfo> previousSegments;
	private static int count = 0;

	public boolean isThisANewVehicle(LRTuple tuple) {
		String csegment = "" + tuple.xway + "_" + tuple.seg;
		boolean isnew = false;
		if (previousSegments.containsKey(tuple.vid)) {
			if (!previousSegments.get(tuple.vid).segment.equals(csegment)) {
				isnew = true;
			}
		} else {
			isnew = true;
		}
		previousSegments.put(tuple.vid, new SegmentInfo(csegment, tuple.time));

		if (count == 50) {
			count = 0;
			long time = System.currentTimeMillis();
			ArrayList<Integer> toRemove = new ArrayList<Integer>();
			for (Integer vt : this.previousSegments.keySet()) {
				if (time - this.previousSegments.get(vt).lastTimeUpdate > 30) {
					toRemove.add(vt);
				}
			}
			for (Integer vt : toRemove)
				previousSegments.remove(vt);
		}
		return isnew;
	}

}