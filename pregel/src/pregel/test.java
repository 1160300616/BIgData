package pregel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		vertex v1 = new vertex(1);
		vertex v2 = new vertex(1);
		v2 = v1;
		v1.setValue(3);
		System.out.println(v2.getValue());
		
	}

}
