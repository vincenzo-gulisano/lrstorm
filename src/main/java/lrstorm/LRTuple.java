package lrstorm;

import java.io.Serializable;
import java.util.regex.Pattern;


public class LRTuple implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6655042762837984179L;
	private static final Pattern SPACE = Pattern.compile(",");
	// Type = 0, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
	public int type;
	public long time;
	public int vid;
	public int speed;
	public int xway;
	public int lane;
	public int dir;
	public int seg;
	public int pos;
	
	public LRTuple(String input) {
		String[] elems = SPACE.split(input.trim());
		type = Integer.parseInt(elems[0]);
		time = Long.parseLong(elems[1]);
		vid = Integer.parseInt(elems[2]);
		speed = Integer.parseInt(elems[3]);
		xway = Integer.parseInt(elems[4]);
		lane = Integer.parseInt(elems[5]);
		dir = Integer.parseInt(elems[6]);
		seg = Integer.parseInt(elems[7]);
		pos = Integer.parseInt(elems[8]);
	}

	@Override
	public String toString() {
		return type + "," + time + "," + vid
				+ "," + speed + "," + xway + "," + lane
				+ "," + dir + "," + seg + "," + pos;
	}
	
	

}