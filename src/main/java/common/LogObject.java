package common;

import java.io.Serializable;

public class LogObject implements Serializable {

	public int count;
	public int liveNodes;
	public long start;
	public long end;
	
	public LogObject(int count,int liveNodes,long start,long end) {
		this.count = count;
		this.liveNodes = liveNodes;
		this.start = start;
		this.end = end;
		
	}
	
}
