package pregel;

import java.util.ArrayList;
import java.util.HashMap;
import pregel.worker;
import java.util.Map;

public class communication {
	
	public static Map<worker, HashMap<vertex, ArrayList<Integer>>> receiveMsg = new HashMap<worker,HashMap<vertex,ArrayList<Integer>>>();
	public static Map<worker, HashMap<vertex, ArrayList<Integer>>> sendMsg = new HashMap<worker,HashMap<vertex,ArrayList<Integer>>>();
}
