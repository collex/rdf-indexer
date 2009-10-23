package org.nines;

import java.io.FileWriter;
import java.io.IOException;

public class LinkCollector {
	
	private FileWriter linkDataFile; 

	public LinkCollector(String prefix) {
		try {
			linkDataFile = new FileWriter(prefix + "_link_data.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void addLink( String documentURI, String filename, String url ) {
		try {
			linkDataFile.write(documentURI+"\t"+filename+"\t"+url+"\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void close( ) {
		try {
			linkDataFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
